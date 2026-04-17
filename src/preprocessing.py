"""Data cleaning and preprocessing for telemetry."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.config import (
    OPERATING_MODES,
    PLAUSIBLE_RANGES,
    PREPROCESSING_REPORT_PATH,
    PROCESSED_TELEMETRY_CSV_PATH,
    PROCESSED_TELEMETRY_PARQUET_PATH,
)


def _normalize_operating_mode(mode_series: pd.Series) -> tuple[pd.Series, int]:
    normalized = mode_series.astype(str).str.strip().str.lower()
    invalid_mask = ~normalized.isin(OPERATING_MODES)
    invalid_count = int(invalid_mask.sum())
    normalized.loc[invalid_mask] = "normal"
    return normalized, invalid_count


def _coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = list(PLAUSIBLE_RANGES.keys())
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _impute_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = [c for c in PLAUSIBLE_RANGES if c in df.columns]

    for col in numeric_cols:
        df[col] = df.groupby("miner_id", group_keys=False)[col].transform(
            lambda s: s.ffill().bfill()
        )
        median_value = df[col].median()
        df[col] = df[col].fillna(median_value)
    return df


def _clip_to_ranges(df: pd.DataFrame) -> dict[str, int]:
    clipped_counts: dict[str, int] = {}
    for col, (lower, upper) in PLAUSIBLE_RANGES.items():
        if col not in df.columns:
            continue
        before = df[col].copy()
        df[col] = df[col].clip(lower=lower, upper=upper)
        clipped_counts[col] = int((before != df[col]).sum())
    return clipped_counts


def _recompute_efficiency_and_deviation(df: pd.DataFrame) -> pd.DataFrame:
    df["efficiency_j_per_th"] = df["asic_power_w"] / df["asic_hashrate_ths"].replace(0, np.nan)
    df["efficiency_j_per_th"] = df["efficiency_j_per_th"].replace([np.inf, -np.inf], np.nan)
    df["efficiency_j_per_th"] = df["efficiency_j_per_th"].fillna(
        df["efficiency_j_per_th"].median()
    )

    baseline_hashrate = df.groupby(["miner_id", "operating_mode"])["asic_hashrate_ths"].transform(
        "median"
    )
    baseline_hashrate = baseline_hashrate.replace(0, np.nan).fillna(df["asic_hashrate_ths"].median())
    df["hashrate_deviation_pct"] = (
        (df["asic_hashrate_ths"] - baseline_hashrate) / baseline_hashrate
    ) * 100.0
    df["hashrate_deviation_pct"] = df["hashrate_deviation_pct"].clip(
        PLAUSIBLE_RANGES["hashrate_deviation_pct"][0],
        PLAUSIBLE_RANGES["hashrate_deviation_pct"][1],
    )
    return df


def preprocess_telemetry(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object]]:
    """Clean raw telemetry and return cleaned frame plus processing report."""

    working = df.copy()
    initial_rows = int(len(working))

    working["timestamp"] = pd.to_datetime(working["timestamp"], errors="coerce")
    null_timestamp_rows = int(working["timestamp"].isna().sum())
    working = working.dropna(subset=["timestamp", "miner_id"]).copy()
    dropped_invalid_core_rows = initial_rows - len(working)

    working["operating_mode"], replaced_modes = _normalize_operating_mode(
        working["operating_mode"]
    )
    working = _coerce_numeric(working)

    duplicate_count = int(working.duplicated(subset=["timestamp", "miner_id"]).sum())
    working = (
        working.sort_values(["miner_id", "timestamp"])
        .drop_duplicates(subset=["timestamp", "miner_id"], keep="last")
        .reset_index(drop=True)
    )

    pre_impute_missing = {
        c: int(working[c].isna().sum())
        for c in working.columns
        if c in PLAUSIBLE_RANGES or c in ("timestamp", "miner_id", "operating_mode")
    }
    working = _impute_missing_values(working)

    clipped_counts = _clip_to_ranges(working)
    working = _recompute_efficiency_and_deviation(working)

    working["failure_within_horizon"] = (
        working["failure_within_horizon"].fillna(0).round().astype(int).clip(0, 1)
    )

    final_missing = {c: int(working[c].isna().sum()) for c in working.columns}
    report: dict[str, object] = {
        "input_rows": initial_rows,
        "output_rows": int(len(working)),
        "rows_dropped_invalid_core_fields": int(dropped_invalid_core_rows),
        "null_timestamp_rows": null_timestamp_rows,
        "duplicates_removed": duplicate_count,
        "operating_mode_replacements_to_normal": replaced_modes,
        "missing_values_before_imputation": pre_impute_missing,
        "missing_values_after_preprocessing": final_missing,
        "clipped_values_by_column": clipped_counts,
        "label_positive_rate": float(working["failure_within_horizon"].mean()),
        "miners_count": int(working["miner_id"].nunique()),
        "start_timestamp": str(working["timestamp"].min()),
        "end_timestamp": str(working["timestamp"].max()),
    }
    return working, report


def save_processed_telemetry(
    cleaned_df: pd.DataFrame,
    parquet_path: str | Path = PROCESSED_TELEMETRY_PARQUET_PATH,
    csv_path: str | Path = PROCESSED_TELEMETRY_CSV_PATH,
) -> dict[str, str]:
    """Persist cleaned telemetry. Writes parquet when possible and CSV always."""

    written_paths: dict[str, str] = {}
    parquet_out = Path(parquet_path)
    csv_out = Path(csv_path)
    parquet_out.parent.mkdir(parents=True, exist_ok=True)
    csv_out.parent.mkdir(parents=True, exist_ok=True)

    try:
        cleaned_df.to_parquet(parquet_out, index=False)
        written_paths["parquet"] = str(parquet_out)
    except Exception as exc:  # pragma: no cover
        written_paths["parquet_error"] = str(exc)

    cleaned_df.to_csv(csv_out, index=False)
    written_paths["csv"] = str(csv_out)
    return written_paths


def run_preprocessing(
    raw_df: pd.DataFrame,
    report_path: str | Path = PREPROCESSING_REPORT_PATH,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Run preprocessing and write a JSON summary report."""

    cleaned_df, report = preprocess_telemetry(raw_df)
    report_out = Path(report_path)
    report_out.parent.mkdir(parents=True, exist_ok=True)
    with report_out.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    return cleaned_df, report
