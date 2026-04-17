"""Phase 5 evaluation, threshold analysis, and reporting outputs."""

from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

from src.config import (
    PHASE4_BEST_MODEL_SUMMARY_PATH,
    PHASE4_RISK_PREDICTIONS_PATH,
    PHASE5_EVALUATION_SUMMARY_PATH,
    PHASE5_FLAGGED_MINERS_PATH,
    PHASE5_POLICY_BACKTEST_PATH,
    PHASE5_PRIORITY_ALERTS_PATH,
    PHASE5_THRESHOLD_TABLE_PATH,
)
from src.evaluation import (
    build_threshold_analysis_table,
    evaluate_at_threshold,
    get_classification_report_dict,
    get_confusion_matrix_array,
)
from src.visualization import (
    plot_feature_importance,
    plot_mode_performance_breakdown,
    plot_phase5_confusion_matrix,
    plot_phase5_pr_curve,
    plot_phase5_risk_distribution,
    plot_phase5_roc_curve,
)
from src.policy import backtest_policy_uplift, parse_policy_config


def _load_recommended_threshold(default: float = 0.5) -> float:
    if not PHASE4_BEST_MODEL_SUMMARY_PATH.exists():
        return default

    try:
        with Path(PHASE4_BEST_MODEL_SUMMARY_PATH).open("r", encoding="utf-8") as f:
            data = json.load(f)
        return float(data.get("threshold_for_alerting", default))
    except Exception:
        return default


def _load_policy_config_from_env() -> dict:
    return parse_policy_config(
        {
            "policy_optimizer_enabled": os.getenv("POLICY_OPTIMIZER_ENABLED", "true"),
            "automation_require_policy_backtest": os.getenv(
                "AUTOMATION_REQUIRE_POLICY_BACKTEST", "true"
            ),
            "policy_min_uplift_usd_per_miner": os.getenv(
                "POLICY_MIN_UPLIFT_USD_PER_MINER", "0.25"
            ),
            "energy_price_usd_per_kwh": os.getenv("ENERGY_PRICE_USD_PER_KWH", "0.08"),
            "hashprice_usd_per_ph_day": os.getenv("HASHPRICE_USD_PER_PH_DAY", "55"),
            "opex_usd_per_mwh": os.getenv("OPEX_USD_PER_MWH", "8"),
            "capex_usd_per_mwh": os.getenv("CAPEX_USD_PER_MWH", "20"),
            "energy_price_schedule_json": os.getenv("ENERGY_PRICE_SCHEDULE_JSON", "{}"),
            "curtailment_windows_json": os.getenv("CURTAILMENT_WINDOWS_JSON", "[]"),
            "curtailment_penalty_multiplier": os.getenv(
                "CURTAILMENT_PENALTY_MULTIPLIER", "2.0"
            ),
            "policy_reward_per_th_hour_usd": os.getenv(
                "POLICY_REWARD_PER_TH_HOUR_USD", "0.0022916667"
            ),
            "policy_failure_cost_usd": os.getenv("POLICY_FAILURE_COST_USD", "300"),
            "policy_horizon_hours": os.getenv("POLICY_HORIZON_HOURS", "1.0"),
            "risk_probability_horizon_hours": os.getenv(
                "RISK_PROBABILITY_HORIZON_HOURS", "24"
            ),
            "policy_timezone": os.getenv("POLICY_TIMEZONE", "UTC"),
        }
    )


def _build_flagged_miner_summary(risk_df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    working = risk_df.copy()
    working["flagged"] = (working["risk_score"] >= threshold).astype(int)
    working["critical_flag"] = (working["risk_score"] >= 0.75).astype(int)

    agg = (
        working.groupby("miner_id")
        .agg(
            max_risk_score=("risk_score", "max"),
            mean_risk_score=("risk_score", "mean"),
            flagged_rows=("flagged", "sum"),
            critical_rows=("critical_flag", "sum"),
            validation_rows=("risk_score", "count"),
            true_failure_rate=("failure_within_horizon", "mean"),
        )
        .reset_index()
    )
    agg["flagged_rate"] = agg["flagged_rows"] / agg["validation_rows"].replace(0, np.nan)
    agg["priority_score"] = 0.65 * agg["max_risk_score"] + 0.35 * agg["mean_risk_score"]
    agg = agg.sort_values(["priority_score", "critical_rows"], ascending=[False, False])
    return agg


def _build_priority_alerts(risk_df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    working = risk_df.copy()
    if "alert_signal_count" not in working.columns:
        flag_cols = [
            c
            for c in [
                "flag_high_temp",
                "flag_power_unstable",
                "flag_hashrate_drop",
                "flag_low_te",
            ]
            if c in working.columns
        ]
        if flag_cols:
            working["alert_signal_count"] = working[flag_cols].sum(axis=1)
        else:
            working["alert_signal_count"] = 0

    working["priority_alert"] = (
        (working["risk_score"] >= threshold) & (working["alert_signal_count"] >= 1)
    ) | (working["risk_score"] >= 0.80)

    cols = [
        "timestamp",
        "miner_id",
        "operating_mode",
        "risk_score",
        "risk_band",
        "failure_within_horizon",
        "predicted_failure_risk",
        "alert_signal_count",
        "asic_temperature_c",
        "power_instability_index",
        "hashrate_degradation_pct_12h",
        "te_drift_pct_4h",
    ]
    cols = [c for c in cols if c in working.columns]

    priority = working.loc[working["priority_alert"], cols].copy()
    priority = priority.sort_values(["risk_score", "alert_signal_count"], ascending=[False, False])
    return priority.head(400)


def run_phase5_evaluation() -> dict[str, object]:
    """Run Phase 5 evaluation/reporting using Phase 4 validation predictions."""

    if not PHASE4_RISK_PREDICTIONS_PATH.exists():
        raise FileNotFoundError(
            f"Missing Phase 4 risk predictions at {PHASE4_RISK_PREDICTIONS_PATH}. "
            "Run phase4 first."
        )

    risk_df = pd.read_csv(PHASE4_RISK_PREDICTIONS_PATH)
    y_true = pd.to_numeric(risk_df["failure_within_horizon"], errors="coerce").fillna(0).astype(int)
    y_score = pd.to_numeric(risk_df["risk_score"], errors="coerce").fillna(0.0).astype(float)

    recommended_threshold = _load_recommended_threshold(default=0.5)

    threshold_table = build_threshold_analysis_table(y_true.to_numpy(), y_score.to_numpy())
    threshold_table.to_csv(PHASE5_THRESHOLD_TABLE_PATH, index=False)

    policy_cfg = _load_policy_config_from_env()
    policy_backtest = backtest_policy_uplift(
        rows=[dict(row) for row in risk_df.to_dict(orient="records")],
        cfg=policy_cfg,
    )
    PHASE5_POLICY_BACKTEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with Path(PHASE5_POLICY_BACKTEST_PATH).open("w", encoding="utf-8") as f:
        json.dump(
            {"policy": policy_cfg, "backtest": policy_backtest},
            f,
            indent=2,
        )

    metrics_at_recommended = evaluate_at_threshold(
        y_true.to_numpy(), y_score.to_numpy(), recommended_threshold
    )
    cm = get_confusion_matrix_array(y_true.to_numpy(), y_score.to_numpy(), recommended_threshold)
    class_report = get_classification_report_dict(
        y_true.to_numpy(), y_score.to_numpy(), recommended_threshold
    )

    # Conservative threshold candidate: precision >= 0.40, maximize recall.
    conservative_candidates = threshold_table[threshold_table["precision"] >= 0.40]
    if len(conservative_candidates) > 0:
        conservative_pick = conservative_candidates.sort_values(
            ["recall", "f1"], ascending=[False, False]
        ).iloc[0]
    else:
        conservative_pick = threshold_table.sort_values("f1", ascending=False).iloc[0]

    flagged_miners = _build_flagged_miner_summary(risk_df, recommended_threshold)
    flagged_miners.to_csv(PHASE5_FLAGGED_MINERS_PATH, index=False)

    priority_alerts = _build_priority_alerts(risk_df, recommended_threshold)
    priority_alerts.to_csv(PHASE5_PRIORITY_ALERTS_PATH, index=False)

    figure_paths = {
        "confusion_matrix": plot_phase5_confusion_matrix(cm),
        "precision_recall_curve": plot_phase5_pr_curve(y_true.to_numpy(), y_score.to_numpy()),
        "roc_curve": plot_phase5_roc_curve(y_true.to_numpy(), y_score.to_numpy()),
        "risk_distribution": plot_phase5_risk_distribution(risk_df),
        "mode_performance": plot_mode_performance_breakdown(risk_df, recommended_threshold),
    }
    try:
        figure_paths["feature_importance"] = plot_feature_importance()
    except Exception:
        pass  # Feature importance CSV may not exist if phase4 was skipped

    summary = {
        "validation_rows": int(len(risk_df)),
        "positive_label_rate": float(y_true.mean()),
        "recommended_threshold": float(recommended_threshold),
        "metrics_at_recommended_threshold": metrics_at_recommended,
        "classification_report": class_report,
        "conservative_threshold_candidate": {
            "threshold": float(conservative_pick["threshold"]),
            "precision": float(conservative_pick["precision"]),
            "recall": float(conservative_pick["recall"]),
            "f1": float(conservative_pick["f1"]),
            "alert_rate": float(conservative_pick["alert_rate"]),
        },
        "flagged_miners_count": int((flagged_miners["flagged_rows"] > 0).sum()),
        "priority_alert_rows": int(len(priority_alerts)),
        "top_priority_miners": flagged_miners.head(10)[
            ["miner_id", "priority_score", "max_risk_score", "flagged_rows", "critical_rows"]
        ].to_dict(orient="records"),
        "policy_backtest": policy_backtest,
        "outputs": {
            "threshold_analysis_csv": str(PHASE5_THRESHOLD_TABLE_PATH),
            "flagged_miners_csv": str(PHASE5_FLAGGED_MINERS_PATH),
            "priority_alerts_csv": str(PHASE5_PRIORITY_ALERTS_PATH),
            "policy_backtest_json": str(PHASE5_POLICY_BACKTEST_PATH),
            "figure_paths": figure_paths,
        },
    }

    PHASE5_EVALUATION_SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with Path(PHASE5_EVALUATION_SUMMARY_PATH).open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return {
        "evaluation_summary_json": str(PHASE5_EVALUATION_SUMMARY_PATH),
        "threshold_analysis_csv": str(PHASE5_THRESHOLD_TABLE_PATH),
        "flagged_miners_csv": str(PHASE5_FLAGGED_MINERS_PATH),
        "priority_alerts_csv": str(PHASE5_PRIORITY_ALERTS_PATH),
        "policy_backtest_json": str(PHASE5_POLICY_BACKTEST_PATH),
        "figure_paths": figure_paths,
        "recommended_threshold": float(recommended_threshold),
    }


if __name__ == "__main__":
    result = run_phase5_evaluation()
    print("Phase 5 completed.")
    print(json.dumps(result, indent=2))
