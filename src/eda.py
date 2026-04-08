"""Exploratory analysis, anomaly detection, and Phase 3 exports."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

from config import (
    ANOMALIES_OUTPUT_PATH,
    CORRELATION_MATRIX_CSV_PATH,
    EDA_SUMMARY_PATH,
    TRADEOFF_SUMMARY_CSV_PATH,
)
from visualization import build_phase3_figures


EDA_NUMERIC_COLUMNS = [
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
    "failure_within_horizon",
]


def compute_correlation_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Compute correlation matrix over key telemetry and KPI columns."""

    available_cols = [c for c in EDA_NUMERIC_COLUMNS if c in df.columns]
    return df[available_cols].corr(numeric_only=True)


def compute_tradeoff_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize operational trade-offs by operating mode."""

    group_cols = [
        "asic_hashrate_ths",
        "asic_power_w",
        "cooling_power_w",
        "asic_temperature_c",
        "efficiency_j_per_th",
        "true_efficiency_te",
        "power_instability_index",
        "failure_within_horizon",
    ]
    summary = (
        df.groupby("operating_mode")[group_cols]
        .agg(["mean", "median", "std"])
        .round(4)
    )
    summary.columns = ["_".join(c).strip() for c in summary.columns]
    return summary.reset_index()


def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Flag rows showing multi-signal stress/anomaly behavior."""

    working = df.copy()

    temp_z = (
        (working["asic_temperature_c"] - working["asic_temperature_c"].mean())
        / (working["asic_temperature_c"].std(ddof=0) + 1e-6)
    )
    power_z = (
        (working["asic_power_w"] - working["asic_power_w"].mean())
        / (working["asic_power_w"].std(ddof=0) + 1e-6)
    )
    te_low_threshold = working["true_efficiency_te"].quantile(0.10)

    working["anomaly_temp"] = (working["asic_temperature_c"] > 95.0) | (temp_z > 2.8)
    working["anomaly_power_instability"] = working["power_instability_index"] > 0.75
    working["anomaly_hashrate_drop"] = working["hashrate_deviation_pct"] < -15.0
    working["anomaly_power_spike"] = power_z > 2.8
    working["anomaly_low_te"] = working["true_efficiency_te"] < te_low_threshold

    anomaly_flags = [
        "anomaly_temp",
        "anomaly_power_instability",
        "anomaly_hashrate_drop",
        "anomaly_power_spike",
        "anomaly_low_te",
    ]
    working["anomaly_signal_count"] = working[anomaly_flags].sum(axis=1)
    working["is_anomaly"] = working["anomaly_signal_count"] >= 2

    cols = [
        "timestamp",
        "miner_id",
        "operating_mode",
        "asic_temperature_c",
        "asic_power_w",
        "asic_hashrate_ths",
        "power_instability_index",
        "hashrate_deviation_pct",
        "true_efficiency_te",
        "failure_within_horizon",
        "anomaly_signal_count",
    ] + anomaly_flags
    anomalies = working.loc[working["is_anomaly"], cols].copy()
    anomalies = anomalies.sort_values(
        ["anomaly_signal_count", "timestamp"], ascending=[False, True]
    )
    return anomalies


def build_eda_summary(
    df: pd.DataFrame, corr_df: pd.DataFrame, anomalies_df: pd.DataFrame
) -> Dict[str, object]:
    """Build high-level EDA summary for reporting."""

    key_pairs = [
        ("asic_clock_mhz", "asic_hashrate_ths"),
        ("asic_voltage_v", "asic_power_w"),
        ("ambient_temperature_c", "asic_temperature_c"),
        ("asic_power_w", "asic_temperature_c"),
        ("power_instability_index", "failure_within_horizon"),
        ("true_efficiency_te", "failure_within_horizon"),
    ]

    correlations = {}
    for a, b in key_pairs:
        if a in corr_df.index and b in corr_df.columns:
            correlations[f"{a}__{b}"] = float(corr_df.loc[a, b])

    anomaly_rate = float(len(anomalies_df) / max(len(df), 1))
    top_anomaly_miners = (
        anomalies_df["miner_id"].value_counts().head(10).to_dict()
        if len(anomalies_df) > 0
        else {}
    )

    return {
        "rows_analyzed": int(len(df)),
        "miners_analyzed": int(df["miner_id"].nunique()),
        "label_positive_rate": float(df["failure_within_horizon"].mean()),
        "anomaly_rows": int(len(anomalies_df)),
        "anomaly_rate": anomaly_rate,
        "key_correlations": correlations,
        "top_anomaly_miners": top_anomaly_miners,
    }


def run_eda_pipeline(df_with_kpi: pd.DataFrame) -> Dict[str, object]:
    """Run correlation, anomalies, trade-off analysis, and figure generation."""

    corr_df = compute_correlation_matrix(df_with_kpi)
    tradeoff_df = compute_tradeoff_summary(df_with_kpi)
    anomalies_df = detect_anomalies(df_with_kpi)
    summary = build_eda_summary(df_with_kpi, corr_df, anomalies_df)

    CORRELATION_MATRIX_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    corr_df.to_csv(CORRELATION_MATRIX_CSV_PATH, index=True)
    tradeoff_df.to_csv(TRADEOFF_SUMMARY_CSV_PATH, index=False)

    ANOMALIES_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    anomalies_df.to_csv(ANOMALIES_OUTPUT_PATH, index=False)

    EDA_SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with Path(EDA_SUMMARY_PATH).open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    figure_paths = build_phase3_figures(corr_df=corr_df, df_with_kpi=df_with_kpi)

    return {
        "correlation_csv": str(CORRELATION_MATRIX_CSV_PATH),
        "tradeoff_summary_csv": str(TRADEOFF_SUMMARY_CSV_PATH),
        "anomalies_csv": str(ANOMALIES_OUTPUT_PATH),
        "eda_summary_json": str(EDA_SUMMARY_PATH),
        "figure_paths": figure_paths,
        "anomaly_count": int(len(anomalies_df)),
    }
