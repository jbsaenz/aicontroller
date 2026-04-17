"""End-to-end pipeline runner."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from src.config import (
    INGESTION_REPORT_PATH,
    KPI_TELEMETRY_CSV_PATH,
    OUTPUTS_METRICS_DIR,
    PREPROCESSING_REPORT_PATH,
    PROCESSED_TELEMETRY_CSV_PATH,
    RAW_TELEMETRY_PATH,
    DataGenerationConfig,
)
from src.data_generation import generate_synthetic_telemetry, summarize_generated_data
from src.eda import run_eda_pipeline
from src.feature_engineering import run_feature_engineering
from src.ingestion import run_ingestion
from src.kpi import run_kpi_pipeline
from src.phase5 import run_phase5_evaluation
from src.preprocessing import run_preprocessing, save_processed_telemetry
from src.train import run_training_pipeline


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def run_phase2_data_pipeline(
    n_miners: int = 48,
    days: int = 21,
    freq_minutes: int = 10,
    seed: int = 42,
    prediction_horizon_hours: int = 24,
) -> dict[str, object]:
    """Generate synthetic telemetry, validate it, and produce cleaned dataset."""

    cfg = DataGenerationConfig(
        n_miners=n_miners,
        days=days,
        freq_minutes=freq_minutes,
        seed=seed,
        prediction_horizon_hours=prediction_horizon_hours,
    )

    OUTPUTS_METRICS_DIR.mkdir(parents=True, exist_ok=True)

    raw_df = generate_synthetic_telemetry(cfg)
    RAW_TELEMETRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    raw_df.to_csv(RAW_TELEMETRY_PATH, index=False)
    generation_summary = summarize_generated_data(raw_df, cfg)
    _write_json(OUTPUTS_METRICS_DIR / "phase2_generation_summary.json", generation_summary)

    ingested_df, ingestion_report = run_ingestion(
        csv_path=RAW_TELEMETRY_PATH, report_path=INGESTION_REPORT_PATH
    )
    cleaned_df, preprocessing_report = run_preprocessing(
        raw_df=ingested_df,
        report_path=PREPROCESSING_REPORT_PATH,
    )
    written_paths = save_processed_telemetry(cleaned_df)

    return {
        "generation_summary": generation_summary,
        "ingestion_report_path": str(INGESTION_REPORT_PATH),
        "preprocessing_report_path": str(PREPROCESSING_REPORT_PATH),
        "processed_outputs": written_paths,
    }


def run_phase3_analysis() -> dict[str, object]:
    """Run Tier 1 EDA + TE KPI analysis on processed telemetry."""

    if not PROCESSED_TELEMETRY_CSV_PATH.exists():
        raise FileNotFoundError(
            f"Processed telemetry not found at {PROCESSED_TELEMETRY_CSV_PATH}. "
            "Run phase2 first or use --phase phase2-3."
        )

    df = pd.read_csv(PROCESSED_TELEMETRY_CSV_PATH)
    df_with_kpi, kpi_summary, kpi_written_paths = run_kpi_pipeline(df)
    eda_results = run_eda_pipeline(df_with_kpi)

    return {
        "input_processed_csv": str(PROCESSED_TELEMETRY_CSV_PATH),
        "kpi_summary": kpi_summary,
        "kpi_outputs": kpi_written_paths,
        "eda_outputs": eda_results,
    }


def run_phase4_modeling() -> dict[str, object]:
    """Run Phase 4 feature engineering and baseline model training."""

    if not KPI_TELEMETRY_CSV_PATH.exists():
        run_phase3_analysis()

    kpi_df = pd.read_csv(KPI_TELEMETRY_CSV_PATH)
    features_df, feature_cols, feature_summary, feature_written = run_feature_engineering(
        kpi_df
    )
    training_result = run_training_pipeline(
        features_df=features_df,
        feature_cols=feature_cols,
        target_col="failure_within_horizon",
    )

    return {
        "input_kpi_csv": str(KPI_TELEMETRY_CSV_PATH),
        "feature_summary": feature_summary,
        "feature_outputs": feature_written,
        "training_outputs": training_result,
    }


def run_phase5_reporting() -> dict[str, object]:
    """Run Phase 5 evaluation/reporting on Phase 4 outputs."""

    phase4_result = run_phase4_modeling()
    phase5_result = run_phase5_evaluation()
    return {
        "phase4_dependency": phase4_result,
        "phase5_outputs": phase5_result,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run project pipeline phases."
    )
    parser.add_argument(
        "--phase",
        choices=[
            "phase2",
            "phase3",
            "phase4",
            "phase5",
            "phase2-3",
            "phase2-4",
            "phase2-5",
        ],
        default="phase2",
    )
    parser.add_argument("--n-miners", type=int, default=48)
    parser.add_argument("--days", type=int, default=21)
    parser.add_argument("--freq-minutes", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--prediction-horizon-hours", type=int, default=24)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.phase == "phase2":
        result = run_phase2_data_pipeline(
            n_miners=args.n_miners,
            days=args.days,
            freq_minutes=args.freq_minutes,
            seed=args.seed,
            prediction_horizon_hours=args.prediction_horizon_hours,
        )
        print("Phase 2 completed.")
        print(json.dumps(result, indent=2))
    elif args.phase == "phase3":
        result = run_phase3_analysis()
        print("Phase 3 completed.")
        print(json.dumps(result, indent=2))
    elif args.phase == "phase2-3":
        phase2_result = run_phase2_data_pipeline(
            n_miners=args.n_miners,
            days=args.days,
            freq_minutes=args.freq_minutes,
            seed=args.seed,
            prediction_horizon_hours=args.prediction_horizon_hours,
        )
        phase3_result = run_phase3_analysis()
        combined = {
            "phase2": phase2_result,
            "phase3": phase3_result,
        }
        print("Phase 2 and Phase 3 completed.")
        print(json.dumps(combined, indent=2))
    elif args.phase == "phase4":
        result = run_phase4_modeling()
        print("Phase 4 completed.")
        print(json.dumps(result, indent=2))
    elif args.phase == "phase2-4":
        phase2_result = run_phase2_data_pipeline(
            n_miners=args.n_miners,
            days=args.days,
            freq_minutes=args.freq_minutes,
            seed=args.seed,
            prediction_horizon_hours=args.prediction_horizon_hours,
        )
        phase3_result = run_phase3_analysis()
        phase4_result = run_phase4_modeling()
        combined = {
            "phase2": phase2_result,
            "phase3": phase3_result,
            "phase4": phase4_result,
        }
        print("Phase 2 to Phase 4 completed.")
        print(json.dumps(combined, indent=2))
    elif args.phase == "phase5":
        result = run_phase5_reporting()
        print("Phase 5 completed.")
        print(json.dumps(result, indent=2))
    elif args.phase == "phase2-5":
        phase2_result = run_phase2_data_pipeline(
            n_miners=args.n_miners,
            days=args.days,
            freq_minutes=args.freq_minutes,
            seed=args.seed,
            prediction_horizon_hours=args.prediction_horizon_hours,
        )
        phase3_result = run_phase3_analysis()
        phase4_result = run_phase4_modeling()
        phase5_result = run_phase5_evaluation()
        combined = {
            "phase2": phase2_result,
            "phase3": phase3_result,
            "phase4": phase4_result,
            "phase5": phase5_result,
        }
        print("Phase 2 to Phase 5 completed.")
        print(json.dumps(combined, indent=2))


if __name__ == "__main__":
    main()
