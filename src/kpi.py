"""True Efficiency (TE) KPI computation."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.config import (
    KPI_SUMMARY_PATH,
    KPI_TELEMETRY_CSV_PATH,
    KPI_TELEMETRY_PARQUET_PATH,
)


MODE_STRESS_FACTOR = {
    "eco": 0.97,
    "normal": 1.00,
    "turbo": 1.08,
}


def compute_true_efficiency(
    df: pd.DataFrame,
    v_ref: float = 12.5,
    t_ref: float = 25.0,
    alpha_v: float = 0.6,
    alpha_e: float = 0.4,
) -> pd.DataFrame:
    """Append TE KPI columns to telemetry dataframe.

    TE formulation:
    - P_total = asic_power_w + cooling_power_w
    - BaseEff = asic_hashrate_ths / P_total
    - TE = BaseEff / (V_stress * E_stress * M_stress)
    """

    working = df.copy()
    working["operating_mode"] = working["operating_mode"].astype(str).str.lower().str.strip()

    total_power = (working["asic_power_w"] + working["cooling_power_w"]).clip(lower=1e-6)
    working["total_power_w"] = total_power
    working["base_eff_th_per_w"] = working["asic_hashrate_ths"] / total_power

    working["voltage_stress_factor"] = (
        1.0
        + alpha_v
        * np.maximum(0.0, (working["asic_voltage_v"] - v_ref) / max(v_ref, 1e-6))
    )
    working["environment_stress_factor"] = (
        1.0
        + alpha_e
        * np.maximum(0.0, (working["ambient_temperature_c"] - t_ref) / 10.0)
    )
    working["mode_stress_factor"] = working["operating_mode"].map(MODE_STRESS_FACTOR).fillna(1.0)

    stress_product = (
        working["voltage_stress_factor"]
        * working["environment_stress_factor"]
        * working["mode_stress_factor"]
    ).clip(lower=1e-6)
    working["true_efficiency_te"] = working["base_eff_th_per_w"] / stress_product
    working["true_efficiency_te"] = working["true_efficiency_te"].replace(
        [np.inf, -np.inf], np.nan
    )
    working["true_efficiency_te"] = working["true_efficiency_te"].fillna(
        working["true_efficiency_te"].median()
    )
    return working


def summarize_kpi(df_with_kpi: pd.DataFrame) -> dict[str, object]:
    """Create KPI summary dictionary for report and metrics export."""

    mode_summary = (
        df_with_kpi.groupby("operating_mode")[
            [
                "true_efficiency_te",
                "efficiency_j_per_th",
                "asic_power_w",
                "cooling_power_w",
                "asic_hashrate_ths",
            ]
        ]
        .mean()
        .round(4)
        .to_dict(orient="index")
    )

    summary: dict[str, object] = {
        "rows": int(len(df_with_kpi)),
        "te_stats": {
            "min": float(df_with_kpi["true_efficiency_te"].min()),
            "mean": float(df_with_kpi["true_efficiency_te"].mean()),
            "median": float(df_with_kpi["true_efficiency_te"].median()),
            "max": float(df_with_kpi["true_efficiency_te"].max()),
        },
        "mode_average_metrics": mode_summary,
        "correlation_with_failure_label": float(
            df_with_kpi["true_efficiency_te"].corr(df_with_kpi["failure_within_horizon"])
        ),
    }
    return summary


def save_kpi_outputs(
    df_with_kpi: pd.DataFrame,
    summary: dict[str, object],
    summary_path: str | Path = KPI_SUMMARY_PATH,
    parquet_path: str | Path = KPI_TELEMETRY_PARQUET_PATH,
    csv_path: str | Path = KPI_TELEMETRY_CSV_PATH,
) -> dict[str, str]:
    """Save KPI-enriched telemetry and KPI summary."""

    written: dict[str, str] = {}

    summary_out = Path(summary_path)
    summary_out.parent.mkdir(parents=True, exist_ok=True)
    with summary_out.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    written["summary_json"] = str(summary_out)

    parquet_out = Path(parquet_path)
    parquet_out.parent.mkdir(parents=True, exist_ok=True)
    try:
        df_with_kpi.to_parquet(parquet_out, index=False)
        written["kpi_parquet"] = str(parquet_out)
    except Exception as exc:  # pragma: no cover
        written["kpi_parquet_error"] = str(exc)

    csv_out = Path(csv_path)
    csv_out.parent.mkdir(parents=True, exist_ok=True)
    df_with_kpi.to_csv(csv_out, index=False)
    written["kpi_csv"] = str(csv_out)
    return written


def run_kpi_pipeline(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object], dict[str, str]]:
    """Compute TE KPI, summarize it, and persist outputs."""

    df_with_kpi = compute_true_efficiency(df)
    summary = summarize_kpi(df_with_kpi)
    written_paths = save_kpi_outputs(df_with_kpi, summary)
    return df_with_kpi, summary, written_paths
