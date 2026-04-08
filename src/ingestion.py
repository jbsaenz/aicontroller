"""Telemetry ingestion and schema validation utilities."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from config import (
    INGESTION_REPORT_PATH,
    NUMERIC_COLUMNS,
    PLAUSIBLE_RANGES,
    RAW_TELEMETRY_PATH,
    REQUIRED_COLUMNS,
)


def load_raw_telemetry(csv_path: str | Path = RAW_TELEMETRY_PATH) -> pd.DataFrame:
    """Load raw telemetry from CSV."""

    return pd.read_csv(csv_path)


def _detect_missing_and_extra_columns(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    extra = [c for c in df.columns if c not in REQUIRED_COLUMNS]
    return missing, extra


def _compute_range_violations(df: pd.DataFrame) -> Dict[str, int]:
    violations: Dict[str, int] = {}
    for col, (lower, upper) in PLAUSIBLE_RANGES.items():
        if col not in df.columns:
            violations[col] = -1
            continue
        col_numeric = pd.to_numeric(df[col], errors="coerce")
        invalid = ((col_numeric < lower) | (col_numeric > upper)).sum()
        violations[col] = int(invalid)
    return violations


def build_ingestion_report(df: pd.DataFrame) -> Dict[str, object]:
    """Build a schema and quality report for raw telemetry."""

    missing_cols, extra_cols = _detect_missing_and_extra_columns(df)
    duplicates_by_key = 0
    if "timestamp" in df.columns and "miner_id" in df.columns:
        duplicates_by_key = int(df.duplicated(subset=["timestamp", "miner_id"]).sum())

    missing_values = {c: int(df[c].isna().sum()) for c in df.columns}
    numeric_parse_nulls = {}
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            parsed = pd.to_numeric(df[col], errors="coerce")
            numeric_parse_nulls[col] = int(parsed.isna().sum())

    report: Dict[str, object] = {
        "rows": int(len(df)),
        "columns_count": int(df.shape[1]),
        "missing_required_columns": missing_cols,
        "extra_columns": extra_cols,
        "duplicates_by_timestamp_miner": duplicates_by_key,
        "missing_values_by_column": missing_values,
        "numeric_parse_nulls_by_column": numeric_parse_nulls,
        "range_violations_by_column": _compute_range_violations(df),
    }
    return report


def validate_required_schema(df: pd.DataFrame) -> None:
    """Raise if required assignment columns are missing."""

    missing, _ = _detect_missing_and_extra_columns(df)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def run_ingestion(
    csv_path: str | Path = RAW_TELEMETRY_PATH,
    report_path: str | Path = INGESTION_REPORT_PATH,
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    """Load telemetry and return dataframe plus schema/quality report."""

    df = load_raw_telemetry(csv_path)
    report = build_ingestion_report(df)
    validate_required_schema(df)

    report_out = Path(report_path)
    report_out.parent.mkdir(parents=True, exist_ok=True)
    with report_out.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    return df, report


def main() -> None:
    df, report = run_ingestion()
    print("Ingestion completed.")
    print(f"rows: {report['rows']}")
    print(f"missing_required_columns: {report['missing_required_columns']}")
    print(f"duplicates_by_timestamp_miner: {report['duplicates_by_timestamp_miner']}")
    print(f"report_path: {INGESTION_REPORT_PATH}")
    print(f"loaded_columns: {list(df.columns)}")


if __name__ == "__main__":
    main()
