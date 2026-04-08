"""Inference and alert generation for predictive-maintenance risk scores."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

from config import PHASE4_ALERTS_PATH, PHASE4_RISK_PREDICTIONS_PATH


def _risk_band(score: float) -> str:
    if score >= 0.75:
        return "critical"
    if score >= 0.55:
        return "high"
    if score >= 0.35:
        return "medium"
    return "low"


def build_risk_outputs(
    validation_df: pd.DataFrame,
    y_score: np.ndarray,
    threshold: float,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build per-row risk predictions and condensed alert examples."""

    output = validation_df.copy()
    output["risk_score"] = y_score
    output["predicted_failure_risk"] = (output["risk_score"] >= threshold).astype(int)
    output["risk_band"] = output["risk_score"].map(_risk_band)
    output["alert_threshold_used"] = threshold

    output["flag_high_temp"] = output.get("asic_temperature_c", 0) >= 95.0
    output["flag_power_unstable"] = output.get("power_instability_index", 0) >= 0.70
    output["flag_hashrate_drop"] = output.get("hashrate_degradation_pct_12h", 0) <= -8.0
    output["flag_low_te"] = output.get("te_drift_pct_4h", 0) <= -8.0
    output["alert_signal_count"] = (
        output["flag_high_temp"].astype(int)
        + output["flag_power_unstable"].astype(int)
        + output["flag_hashrate_drop"].astype(int)
        + output["flag_low_te"].astype(int)
    )

    alert_candidates = output[
        (output["predicted_failure_risk"] == 1) | (output["alert_signal_count"] >= 2)
    ].copy()
    alert_candidates = alert_candidates.sort_values(
        ["risk_score", "alert_signal_count"], ascending=[False, False]
    )

    alert_columns = [
        "timestamp",
        "miner_id",
        "operating_mode",
        "risk_score",
        "risk_band",
        "predicted_failure_risk",
        "failure_within_horizon",
        "asic_temperature_c",
        "power_instability_index",
        "hashrate_degradation_pct_12h",
        "te_drift_pct_4h",
        "alert_signal_count",
        "flag_high_temp",
        "flag_power_unstable",
        "flag_hashrate_drop",
        "flag_low_te",
    ]
    alerts = alert_candidates[alert_columns].head(250).copy()
    return output, alerts


def save_risk_outputs(
    risk_df: pd.DataFrame,
    alerts_df: pd.DataFrame,
    risk_path: str | Path = PHASE4_RISK_PREDICTIONS_PATH,
    alerts_path: str | Path = PHASE4_ALERTS_PATH,
) -> Dict[str, str]:
    """Persist full risk predictions and compact alert examples."""

    written: Dict[str, str] = {}

    risk_out = Path(risk_path)
    risk_out.parent.mkdir(parents=True, exist_ok=True)
    risk_df.to_csv(risk_out, index=False)
    written["risk_predictions_csv"] = str(risk_out)

    alerts_out = Path(alerts_path)
    alerts_out.parent.mkdir(parents=True, exist_ok=True)
    alerts_df.to_csv(alerts_out, index=False)
    written["alerts_csv"] = str(alerts_out)
    return written
