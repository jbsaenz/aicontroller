"""Configuration constants for data generation and pipeline processing."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUTS_FIGURES_DIR = PROJECT_ROOT / "outputs" / "figures"
OUTPUTS_METRICS_DIR = PROJECT_ROOT / "outputs" / "metrics"
OUTPUTS_PREDICTIONS_DIR = PROJECT_ROOT / "outputs" / "predictions"
OUTPUTS_MODELS_DIR = PROJECT_ROOT / "outputs" / "models"

RAW_TELEMETRY_PATH = DATA_RAW_DIR / "synthetic_telemetry.csv"
PROCESSED_TELEMETRY_PARQUET_PATH = DATA_PROCESSED_DIR / "telemetry_clean.parquet"
PROCESSED_TELEMETRY_CSV_PATH = DATA_PROCESSED_DIR / "telemetry_clean.csv"
KPI_TELEMETRY_PARQUET_PATH = DATA_PROCESSED_DIR / "telemetry_with_kpi.parquet"
KPI_TELEMETRY_CSV_PATH = DATA_PROCESSED_DIR / "telemetry_with_kpi.csv"
FEATURES_TELEMETRY_PARQUET_PATH = DATA_PROCESSED_DIR / "telemetry_features.parquet"
FEATURES_TELEMETRY_CSV_PATH = DATA_PROCESSED_DIR / "telemetry_features.csv"
INGESTION_REPORT_PATH = OUTPUTS_METRICS_DIR / "phase2_ingestion_report.json"
PREPROCESSING_REPORT_PATH = OUTPUTS_METRICS_DIR / "phase2_preprocessing_report.json"
KPI_SUMMARY_PATH = OUTPUTS_METRICS_DIR / "phase3_kpi_summary.json"
EDA_SUMMARY_PATH = OUTPUTS_METRICS_DIR / "phase3_eda_summary.json"
CORRELATION_MATRIX_CSV_PATH = OUTPUTS_METRICS_DIR / "phase3_correlation_matrix.csv"
TRADEOFF_SUMMARY_CSV_PATH = OUTPUTS_METRICS_DIR / "phase3_tradeoff_summary.csv"
ANOMALIES_OUTPUT_PATH = OUTPUTS_PREDICTIONS_DIR / "phase3_anomalies.csv"
PHASE4_FEATURE_SUMMARY_PATH = OUTPUTS_METRICS_DIR / "phase4_feature_summary.json"
PHASE4_MODEL_COMPARISON_PATH = OUTPUTS_METRICS_DIR / "phase4_model_comparison.json"
PHASE4_BEST_MODEL_SUMMARY_PATH = OUTPUTS_METRICS_DIR / "phase4_best_model_summary.json"
PHASE4_FEATURE_IMPORTANCE_PATH = OUTPUTS_METRICS_DIR / "phase4_feature_importance.csv"
PHASE4_RISK_PREDICTIONS_PATH = OUTPUTS_PREDICTIONS_DIR / "phase4_validation_risk_scores.csv"
PHASE4_ALERTS_PATH = OUTPUTS_PREDICTIONS_DIR / "phase4_alert_examples.csv"
PHASE4_MODEL_ARTIFACT_PATH = OUTPUTS_MODELS_DIR / "phase4_best_model.joblib"
PHASE5_EVALUATION_SUMMARY_PATH = OUTPUTS_METRICS_DIR / "phase5_evaluation_summary.json"
PHASE5_THRESHOLD_TABLE_PATH = OUTPUTS_METRICS_DIR / "phase5_threshold_analysis.csv"
PHASE5_FLAGGED_MINERS_PATH = OUTPUTS_PREDICTIONS_DIR / "phase5_flagged_miners.csv"
PHASE5_PRIORITY_ALERTS_PATH = OUTPUTS_PREDICTIONS_DIR / "phase5_priority_alerts.csv"
PHASE5_POLICY_BACKTEST_PATH = OUTPUTS_METRICS_DIR / "phase5_policy_backtest.json"

OPERATING_MODES = ("eco", "normal", "turbo")

REQUIRED_COLUMNS = [
    "timestamp",
    "miner_id",
    "operating_mode",
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
    "failure_within_horizon",
]

NUMERIC_COLUMNS = [
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
    "failure_within_horizon",
]

PLAUSIBLE_RANGES = {
    "ambient_temperature_c": (-5.0, 55.0),
    "cooling_power_w": (50.0, 2500.0),
    "asic_clock_mhz": (250.0, 900.0),
    "asic_voltage_v": (10.0, 15.0),
    "asic_hashrate_ths": (5.0, 300.0),
    "asic_temperature_c": (20.0, 125.0),
    "asic_power_w": (500.0, 5000.0),
    "efficiency_j_per_th": (10.0, 80.0),
    "power_instability_index": (0.0, 1.0),
    "hashrate_deviation_pct": (-60.0, 60.0),
    "failure_within_horizon": (0.0, 1.0),
}


@dataclass(frozen=True)
class DataGenerationConfig:
    """Controls synthetic telemetry generation."""

    n_miners: int = 48
    days: int = 21
    freq_minutes: int = 10
    seed: int = 42
    prediction_horizon_hours: int = 24
    start_timestamp: str = "2026-03-01 00:00:00"
    inject_quality_issues: bool = True
    missing_rate: float = 0.003
    duplicate_rate: float = 0.002
