"""Feature engineering for predictive-maintenance modeling."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from config import (
    FEATURES_TELEMETRY_CSV_PATH,
    FEATURES_TELEMETRY_PARQUET_PATH,
    KPI_TELEMETRY_CSV_PATH,
    PHASE4_FEATURE_SUMMARY_PATH,
)


BASE_MODEL_FEATURES = [
    "ambient_temperature_c",
    "cooling_power_w",
    "asic_clock_mhz",
    "asic_voltage_v",
    "asic_hashrate_ths",
    "asic_temperature_c",
    "asic_power_w",
    "efficiency_j_per_th",
    "power_instability_index",
    "hashrate_deviation_pct",
    "true_efficiency_te",
]

ENGINEERED_FEATURES = [
    "temp_roll_mean_1h",
    "temp_roll_max_1h",
    "temp_roll_std_1h",
    "temp_roll_mean_4h",
    "temp_delta_10m",
    "power_instability_roll_mean_1h",
    "power_instability_roll_std_4h",
    "power_delta_10m",
    "hashrate_roll_mean_1h",
    "hashrate_roll_mean_12h",
    "hashrate_degradation_pct_12h",
    "te_roll_mean_4h",
    "te_drift_pct_4h",
    "te_delta_10m",
    "thermal_margin_to_95c",
    "voltage_stress_index",
    "mode_peer_hashrate_dev_pct",
    "mode_peer_temp_dev_c",
    "mode_peer_te_dev_pct",
    "hashrate_residual",
    "chip_temp_imbalance",
]

CATEGORICAL_FEATURES = ["operating_mode"]
TARGET_COLUMN = "failure_within_horizon"
DEFAULT_COOLING_POWER_RATIO = 0.24


def get_model_feature_columns() -> List[str]:
    """Return ordered list of model feature columns."""

    return BASE_MODEL_FEATURES + ENGINEERED_FEATURES + CATEGORICAL_FEATURES


def _safe_pct(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return (numerator / denominator.replace(0, np.nan)) * 100.0


def _ensure_feature_input_columns(
    df: pd.DataFrame, cooling_power_ratio: float = DEFAULT_COOLING_POWER_RATIO
) -> pd.DataFrame:
    """Normalize required input columns for feature engineering.

    This helper is used by both offline training and online serving paths.
    """

    working = df.copy()
    ratio = max(float(cooling_power_ratio), 0.0)

    if "asic_power_w" not in working.columns:
        working["asic_power_w"] = 0.0
    if "asic_hashrate_ths" not in working.columns:
        working["asic_hashrate_ths"] = 0.0
    if "asic_temperature_c" not in working.columns:
        working["asic_temperature_c"] = 0.0
    if "asic_voltage_v" not in working.columns:
        working["asic_voltage_v"] = 12.5
    if "ambient_temperature_c" not in working.columns:
        working["ambient_temperature_c"] = 25.0
    if "operating_mode" not in working.columns:
        working["operating_mode"] = "normal"
    if "power_instability_index" not in working.columns:
        working["power_instability_index"] = 0.0
    if "hashrate_deviation_pct" not in working.columns:
        working["hashrate_deviation_pct"] = 0.0
    if "expected_hashrate_ths" not in working.columns:
        working["expected_hashrate_ths"] = working["asic_hashrate_ths"]
    if "chip_temp_max" not in working.columns:
        working["chip_temp_max"] = working["asic_temperature_c"]

    estimated_cooling = pd.to_numeric(
        working["asic_power_w"], errors="coerce"
    ).fillna(0.0) * ratio
    if "cooling_power_w" not in working.columns:
        working["cooling_power_w"] = estimated_cooling
    else:
        working["cooling_power_w"] = pd.to_numeric(
            working["cooling_power_w"], errors="coerce"
        )
        working["cooling_power_w"] = working["cooling_power_w"].fillna(estimated_cooling)

    if "efficiency_j_per_th" not in working.columns:
        denom = pd.to_numeric(working["asic_hashrate_ths"], errors="coerce").replace(0, np.nan)
        numer = pd.to_numeric(working["asic_power_w"], errors="coerce")
        working["efficiency_j_per_th"] = (numer / denom).replace([np.inf, -np.inf], np.nan)

    if "true_efficiency_te" not in working.columns:
        asic_power = pd.to_numeric(working["asic_power_w"], errors="coerce").fillna(0.0)
        cooling = pd.to_numeric(working["cooling_power_w"], errors="coerce").fillna(0.0)
        total_power = (asic_power + cooling).replace(0, np.nan)
        hashrate = pd.to_numeric(working["asic_hashrate_ths"], errors="coerce")
        working["true_efficiency_te"] = (
            hashrate / total_power
        ).replace([np.inf, -np.inf], np.nan)

    return working


def _build_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    working = _ensure_feature_input_columns(df)
    working["timestamp"] = pd.to_datetime(working["timestamp"], errors="coerce")
    working = working.dropna(subset=["timestamp"]).copy()
    working = working.sort_values(["miner_id", "timestamp"]).reset_index(drop=True)

    g = working.groupby("miner_id", group_keys=False)
    w_1h, w_4h, w_12h = 6, 24, 72

    working["temp_roll_mean_1h"] = g["asic_temperature_c"].transform(
        lambda s: s.rolling(w_1h, min_periods=1).mean()
    )
    working["temp_roll_max_1h"] = g["asic_temperature_c"].transform(
        lambda s: s.rolling(w_1h, min_periods=1).max()
    )
    working["temp_roll_std_1h"] = g["asic_temperature_c"].transform(
        lambda s: s.rolling(w_1h, min_periods=2).std()
    ).fillna(0.0)
    working["temp_roll_mean_4h"] = g["asic_temperature_c"].transform(
        lambda s: s.rolling(w_4h, min_periods=1).mean()
    )
    working["temp_delta_10m"] = g["asic_temperature_c"].diff().fillna(0.0)

    working["power_instability_roll_mean_1h"] = g["power_instability_index"].transform(
        lambda s: s.rolling(w_1h, min_periods=1).mean()
    )
    working["power_instability_roll_std_4h"] = g["power_instability_index"].transform(
        lambda s: s.rolling(w_4h, min_periods=2).std()
    ).fillna(0.0)
    working["power_delta_10m"] = g["asic_power_w"].diff().fillna(0.0)

    working["hashrate_roll_mean_1h"] = g["asic_hashrate_ths"].transform(
        lambda s: s.rolling(w_1h, min_periods=1).mean()
    )
    working["hashrate_roll_mean_12h"] = g["asic_hashrate_ths"].transform(
        lambda s: s.rolling(w_12h, min_periods=1).mean()
    )
    working["hashrate_degradation_pct_12h"] = _safe_pct(
        working["hashrate_roll_mean_1h"] - working["hashrate_roll_mean_12h"],
        working["hashrate_roll_mean_12h"],
    ).fillna(0.0)

    working["te_roll_mean_4h"] = g["true_efficiency_te"].transform(
        lambda s: s.rolling(w_4h, min_periods=1).mean()
    )
    working["te_drift_pct_4h"] = _safe_pct(
        working["true_efficiency_te"] - working["te_roll_mean_4h"],
        working["te_roll_mean_4h"],
    ).fillna(0.0)
    working["te_delta_10m"] = g["true_efficiency_te"].diff().fillna(0.0)

    working["thermal_margin_to_95c"] = 95.0 - working["asic_temperature_c"]
    working["voltage_stress_index"] = np.maximum(
        0.0, (working["asic_voltage_v"] - 12.8) / 12.8
    )

    peer = working.groupby(["timestamp", "operating_mode"])[
        ["asic_hashrate_ths", "asic_temperature_c", "true_efficiency_te"]
    ].transform("mean")
    working["mode_peer_hashrate_dev_pct"] = _safe_pct(
        working["asic_hashrate_ths"] - peer["asic_hashrate_ths"],
        peer["asic_hashrate_ths"],
    ).fillna(0.0)
    working["mode_peer_temp_dev_c"] = (
        working["asic_temperature_c"] - peer["asic_temperature_c"]
    )
    working["mode_peer_te_dev_pct"] = _safe_pct(
        working["true_efficiency_te"] - peer["true_efficiency_te"],
        peer["true_efficiency_te"],
    ).fillna(0.0)

    # Residuals
    working["hashrate_residual"] = (
        working["asic_hashrate_ths"] - working.get("expected_hashrate_ths", df.get("expected_hashrate_ths", working["asic_hashrate_ths"])).fillna(working["asic_hashrate_ths"])
    )
    working["chip_temp_imbalance"] = (
        working.get("chip_temp_max", df.get("chip_temp_max", working["asic_temperature_c"])).fillna(working["asic_temperature_c"]) - working["asic_temperature_c"]
    )
    
    return working


def engineer_features(df_with_kpi: pd.DataFrame) -> Tuple[pd.DataFrame, List[str], Dict[str, object]]:
    """Build modeling features and return feature dataframe and summary."""

    prepared = _ensure_feature_input_columns(df_with_kpi)
    if TARGET_COLUMN not in prepared.columns:
        raise ValueError(f"Expected target column '{TARGET_COLUMN}' in input dataframe.")

    working = _build_temporal_features(prepared)
    feature_cols = get_model_feature_columns()

    numeric_features = [c for c in feature_cols if c not in CATEGORICAL_FEATURES]
    for col in numeric_features:
        working[col] = pd.to_numeric(working[col], errors="coerce")
        working[col] = working[col].fillna(working[col].median())

    working["operating_mode"] = (
        working["operating_mode"].astype(str).str.lower().str.strip().fillna("normal")
    )
    working[TARGET_COLUMN] = (
        pd.to_numeric(working[TARGET_COLUMN], errors="coerce")
        .fillna(0)
        .round()
        .astype(int)
        .clip(0, 1)
    )

    output_columns = ["timestamp", "miner_id"] + feature_cols + [TARGET_COLUMN]
    features_df = working[output_columns].copy()
    summary = {
        "rows": int(len(features_df)),
        "miners": int(features_df["miner_id"].nunique()),
        "feature_count": int(len(feature_cols)),
        "base_feature_count": int(len(BASE_MODEL_FEATURES)),
        "engineered_feature_count": int(len(ENGINEERED_FEATURES)),
        "categorical_feature_count": int(len(CATEGORICAL_FEATURES)),
        "target_positive_rate": float(features_df[TARGET_COLUMN].mean()),
        "start_timestamp": str(features_df["timestamp"].min()),
        "end_timestamp": str(features_df["timestamp"].max()),
    }
    return features_df, feature_cols, summary


def build_serving_feature_snapshot(
    df_with_kpi: pd.DataFrame,
    feature_cols: List[str] | None = None,
    cooling_power_ratio: float = DEFAULT_COOLING_POWER_RATIO,
) -> pd.DataFrame:
    """Build latest per-miner feature rows for online inference."""

    working = _ensure_feature_input_columns(
        df_with_kpi, cooling_power_ratio=cooling_power_ratio
    )
    if TARGET_COLUMN not in working.columns:
        working[TARGET_COLUMN] = 0

    features_df, _, _ = engineer_features(working)
    if features_df.empty:
        return features_df

    latest = (
        features_df.sort_values(["miner_id", "timestamp"])
        .groupby("miner_id", as_index=False, group_keys=False)
        .tail(1)
        .reset_index(drop=True)
    )

    expected_cols = feature_cols or get_model_feature_columns()
    for col in expected_cols:
        if col not in latest.columns:
            latest[col] = np.nan

    keep_cols = ["timestamp", "miner_id"] + expected_cols + [TARGET_COLUMN]
    keep_cols = [col for col in keep_cols if col in latest.columns]
    return latest[keep_cols].copy()


def save_feature_outputs(
    features_df: pd.DataFrame,
    summary: Dict[str, object],
    summary_path: str | Path = PHASE4_FEATURE_SUMMARY_PATH,
    csv_path: str | Path = FEATURES_TELEMETRY_CSV_PATH,
    parquet_path: str | Path = FEATURES_TELEMETRY_PARQUET_PATH,
) -> Dict[str, str]:
    """Persist feature dataset and summary."""

    written: Dict[str, str] = {}

    summary_out = Path(summary_path)
    summary_out.parent.mkdir(parents=True, exist_ok=True)
    with summary_out.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    written["feature_summary_json"] = str(summary_out)

    csv_out = Path(csv_path)
    csv_out.parent.mkdir(parents=True, exist_ok=True)
    features_df.to_csv(csv_out, index=False)
    written["features_csv"] = str(csv_out)

    parquet_out = Path(parquet_path)
    parquet_out.parent.mkdir(parents=True, exist_ok=True)
    try:
        features_df.to_parquet(parquet_out, index=False)
        written["features_parquet"] = str(parquet_out)
    except Exception as exc:  # pragma: no cover
        written["features_parquet_error"] = str(exc)
    return written


def run_feature_engineering(df_with_kpi: pd.DataFrame) -> Tuple[pd.DataFrame, List[str], Dict[str, object], Dict[str, str]]:
    """Full Phase 4 feature engineering workflow."""

    features_df, feature_cols, summary = engineer_features(df_with_kpi)
    written = save_feature_outputs(features_df, summary)
    return features_df, feature_cols, summary, written


def main() -> None:
    if not KPI_TELEMETRY_CSV_PATH.exists():
        raise FileNotFoundError(
            f"KPI dataset not found at {KPI_TELEMETRY_CSV_PATH}. Run phase3 first."
        )

    df = pd.read_csv(KPI_TELEMETRY_CSV_PATH)
    features_df, feature_cols, summary, written = run_feature_engineering(df)
    print("Feature engineering completed.")
    print(f"rows: {len(features_df)}")
    print(f"feature_count: {len(feature_cols)}")
    print(f"target_positive_rate: {summary['target_positive_rate']:.4f}")
    print(f"written_outputs: {written}")


if __name__ == "__main__":
    main()
