"""ML jobs: KPI computation, inference, and model retraining."""

import json
import logging
import os
import threading
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from src.policy import (
    backtest_policy_uplift,
    baseline_policy_decision,
    optimize_policy_decision,
    parse_policy_config,
)
from src.runtime_utils import is_truthy, normalize_control_mode

logger = logging.getLogger("worker.ml_jobs")

MODEL_PATH = Path(
    os.getenv("MODEL_PATH", "/app/outputs/models/phase4_best_model.joblib")
)
MODEL_VERSION = os.getenv("MODEL_VERSION", "v1").strip() or "v1"
RISK_THRESHOLD = float(os.getenv("RISK_THRESHOLD", "0.55"))
ALERT_COOLDOWN_HOURS = int(os.getenv("ALERT_COOLDOWN_HOURS", "1"))
INFERENCE_LOOKBACK_HOURS = int(os.getenv("INFERENCE_LOOKBACK_HOURS", "24"))
COOLING_POWER_RATIO = float(os.getenv("COOLING_POWER_RATIO", "0.24"))
RETRAIN_DAYS = max(int(os.getenv("RETRAIN_DAYS", "30")), 1)
CONTROL_MODE = normalize_control_mode(os.getenv("CONTROL_MODE", "advisory"))
POLICY_BACKTEST_REPORT_PATH = Path(
    os.getenv("POLICY_BACKTEST_REPORT_PATH", "/app/outputs/metrics/policy_backtest_latest.json")
)
POLICY_SETTINGS_KEYS = (
    "policy_optimizer_enabled",
    "automation_require_policy_backtest",
    "policy_min_uplift_usd_per_miner",
    "energy_price_usd_per_kwh",
    "hashprice_usd_per_ph_day",
    "opex_usd_per_mwh",
    "capex_usd_per_mwh",
    "energy_price_schedule_json",
    "curtailment_windows_json",
    "curtailment_penalty_multiplier",
    "policy_reward_per_th_hour_usd",
    "policy_failure_cost_usd",
    "policy_horizon_hours",
    "risk_probability_horizon_hours",
    "policy_timezone",
    "control_mode",
    "inference_lookback_hours",
    "cooling_power_ratio",
    "retrain_days",
)
RUNTIME_SETTINGS_KEYS = (
    "risk_threshold",
    "alert_cooldown_hours",
    *POLICY_SETTINGS_KEYS,
)
RUNTIME_SETTINGS_KEY_LIST = sorted(set(RUNTIME_SETTINGS_KEYS))

# ── KPI constants (from scope plan) ───────────────────────────────────────
V_REF, T_REF = 12.5, 25.0
ALPHA_V, ALPHA_E = 0.6, 0.4
MODE_FACTORS = {"eco": 0.97, "normal": 1.00, "turbo": 1.08}
RISK_BANDS = [(0.75, "critical"), (0.50, "high"), (0.30, "medium"), (0.0, "low")]
KPI_TEXT_COLUMNS = {"miner_id", "operating_mode", "event_codes"}
KPI_INT_COLUMNS = {"failure_within_horizon", "bad_hash_count", "double_hash_count", "read_errors"}
POWER_INSTABILITY_WINDOW = max(int(os.getenv("POWER_INSTABILITY_WINDOW", "6")), 2)
KPI_JOB_BATCH_SIZE = max(int(os.getenv("KPI_JOB_BATCH_SIZE", "5000")), 100)
KPI_JOB_MAX_ROWS_PER_RUN = max(
    int(os.getenv("KPI_JOB_MAX_ROWS_PER_RUN", "50000")),
    KPI_JOB_BATCH_SIZE,
)
RETRAIN_MIN_ROWS = max(int(os.getenv("RETRAIN_MIN_ROWS", "500")), 1)
RETRAIN_MIN_MINERS = max(int(os.getenv("RETRAIN_MIN_MINERS", "10")), 1)
RETRAIN_MIN_TIMESPAN_HOURS = max(
    float(os.getenv("RETRAIN_MIN_TIMESPAN_HOURS", "72")),
    1.0,
)
_RISK_PREDICTIONS_UPSERT_READY = False
_RISK_PREDICTIONS_UPSERT_LOCK = threading.Lock()

# Pre-built at module level — never changes between runs.
_KPI_INSERT_COLS = [
    "timestamp", "miner_id", "asic_clock_mhz", "asic_voltage_v",
    "asic_hashrate_ths", "asic_temperature_c", "asic_power_w",
    "operating_mode", "ambient_temperature_c", "efficiency_j_per_th",
    "power_instability_index", "hashrate_deviation_pct",
    "true_efficiency_te", "failure_within_horizon",
    "chip_temp_max", "chip_temp_std", "bad_hash_count",
    "double_hash_count", "read_errors", "event_codes",
    "expected_hashrate_ths",
]
_KPI_INSERT_SQL = text(
    f"INSERT INTO kpi_telemetry ({', '.join(_KPI_INSERT_COLS)}) "
    f"VALUES ({', '.join(':' + c for c in _KPI_INSERT_COLS)}) "
    "ON CONFLICT (miner_id, timestamp) DO NOTHING"
)


def _risk_band(score: float) -> str:
    for threshold, band in RISK_BANDS:
        if score >= threshold:
            return band
    return "low"


def _compute_te(df: pd.DataFrame) -> pd.Series:
    cooling = df.get("cooling_power_w")
    if cooling is None:
        cooling = df["asic_power_w"].fillna(1500) * COOLING_POWER_RATIO
    cooling = pd.to_numeric(cooling, errors="coerce").fillna(df["asic_power_w"].fillna(0) * COOLING_POWER_RATIO)
    p_total = df["asic_power_w"].fillna(1500) + cooling
    base_eff = df["asic_hashrate_ths"].fillna(1) / p_total.replace(0, np.nan)
    v_stress = 1 + ALPHA_V * np.maximum(0, (df["asic_voltage_v"].fillna(V_REF) - V_REF) / V_REF)
    e_stress = 1 + ALPHA_E * np.maximum(0, (df.get("ambient_temperature_c", pd.Series(T_REF, index=df.index)).fillna(T_REF) - T_REF) / 10)
    mode_factor = df["operating_mode"].fillna("normal").map(MODE_FACTORS).fillna(1.0)
    return (base_eff / (v_stress * e_stress * mode_factor)).round(6)


def _load_runtime_config(engine: Engine) -> dict:
    """Load runtime threshold/cooldown from DB settings with env fallbacks."""
    cfg = {
        "risk_threshold": RISK_THRESHOLD,
        "alert_cooldown_hours": ALERT_COOLDOWN_HOURS,
        "policy_optimizer_enabled": is_truthy(
            os.getenv("POLICY_OPTIMIZER_ENABLED", "true")
        ),
        "automation_require_policy_backtest": is_truthy(
            os.getenv("AUTOMATION_REQUIRE_POLICY_BACKTEST", "true")
        ),
        "policy_min_uplift_usd_per_miner": float(
            os.getenv("POLICY_MIN_UPLIFT_USD_PER_MINER", "0.25")
        ),
        "energy_price_usd_per_kwh": float(
            os.getenv("ENERGY_PRICE_USD_PER_KWH", "0.08")
        ),
        "hashprice_usd_per_ph_day": float(
            os.getenv("HASHPRICE_USD_PER_PH_DAY", "55")
        ),
        "opex_usd_per_mwh": float(os.getenv("OPEX_USD_PER_MWH", "8")),
        "capex_usd_per_mwh": float(os.getenv("CAPEX_USD_PER_MWH", "20")),
        "energy_price_schedule_json": os.getenv("ENERGY_PRICE_SCHEDULE_JSON", "{}"),
        "curtailment_windows_json": os.getenv("CURTAILMENT_WINDOWS_JSON", "[]"),
        "curtailment_penalty_multiplier": float(
            os.getenv("CURTAILMENT_PENALTY_MULTIPLIER", "2.0")
        ),
        "policy_reward_per_th_hour_usd": float(
            os.getenv("POLICY_REWARD_PER_TH_HOUR_USD", "0.0022916667")
        ),
        "policy_failure_cost_usd": float(
            os.getenv("POLICY_FAILURE_COST_USD", "300")
        ),
        "policy_horizon_hours": float(os.getenv("POLICY_HORIZON_HOURS", "1.0")),
        "risk_probability_horizon_hours": float(
            os.getenv("RISK_PROBABILITY_HORIZON_HOURS", "24")
        ),
        "policy_timezone": os.getenv("POLICY_TIMEZONE", "UTC"),
        "control_mode": CONTROL_MODE,
        "inference_lookback_hours": INFERENCE_LOOKBACK_HOURS,
        "cooling_power_ratio": COOLING_POWER_RATIO,
        "retrain_days": RETRAIN_DAYS,
    }
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT key, value
                    FROM app_settings
                    WHERE key = ANY(CAST(:keys AS text[]))
                """),
                {"keys": RUNTIME_SETTINGS_KEY_LIST},
            )
            settings = {r[0]: r[1] for r in result}

        if settings.get("risk_threshold") not in (None, ""):
            parsed = float(settings["risk_threshold"])
            if 0.0 <= parsed <= 1.0:
                cfg["risk_threshold"] = parsed

        if settings.get("alert_cooldown_hours") not in (None, ""):
            parsed = int(float(settings["alert_cooldown_hours"]))
            if parsed >= 0:
                cfg["alert_cooldown_hours"] = parsed

        if settings.get("inference_lookback_hours") not in (None, ""):
            parsed = int(float(settings["inference_lookback_hours"]))
            if parsed >= 1:
                cfg["inference_lookback_hours"] = parsed

        if settings.get("cooling_power_ratio") not in (None, ""):
            parsed = float(settings["cooling_power_ratio"])
            if parsed >= 0:
                cfg["cooling_power_ratio"] = parsed

        if settings.get("retrain_days") not in (None, ""):
            parsed = int(float(settings["retrain_days"]))
            if parsed >= 1:
                cfg["retrain_days"] = parsed

        if settings.get("control_mode") not in (None, ""):
            mode = str(settings["control_mode"]).strip().lower()
            if mode in {"advisory", "actuation"}:
                cfg["control_mode"] = mode

        for key in POLICY_SETTINGS_KEYS:
            if settings.get(key) not in (None, ""):
                cfg[key] = settings[key]
    except Exception as exc:
        logger.warning(
            "Could not load runtime settings from DB, using env defaults: %s", exc
        )

    cfg["control_mode"] = normalize_control_mode(
        cfg.get("control_mode", CONTROL_MODE)
    )
    cfg["inference_lookback_hours"] = max(
        int(float(cfg.get("inference_lookback_hours", INFERENCE_LOOKBACK_HOURS))), 1
    )
    cfg["cooling_power_ratio"] = max(
        float(cfg.get("cooling_power_ratio", COOLING_POWER_RATIO)), 0.0
    )
    cfg["retrain_days"] = max(
        int(float(cfg.get("retrain_days", RETRAIN_DAYS))),
        1,
    )
    cfg["policy"] = parse_policy_config(cfg)
    return cfg


def _prepare_inference_features(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    """Ensure all model features exist, allowing sklearn imputers to fill NaNs."""
    X = df.copy()
    for col in feature_cols:
        if col not in X.columns:
            X[col] = np.nan
    return X[feature_cols]


def _write_policy_backtest_report(report: dict) -> None:
    POLICY_BACKTEST_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with POLICY_BACKTEST_REPORT_PATH.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)


def _normalize_kpi_insert_value(key: str, value):
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass

    if key in KPI_TEXT_COLUMNS:
        text_value = str(value).strip()
        if text_value == "" or text_value.lower() in {"nan", "none", "null"}:
            return None
        return text_value

    if key in KPI_INT_COLUMNS:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if np.isnan(numeric) or np.isinf(numeric):
            return None
        return int(numeric)

    return value


def _build_kpi_insert_records(df: pd.DataFrame, insert_cols: list[str]) -> list[dict]:
    records = []
    for row in df[insert_cols].to_dict(orient="records"):
        cleaned = {}
        for key, value in row.items():
            cleaned[key] = _normalize_kpi_insert_value(key, value)
        records.append(cleaned)
    return records


def _prepare_kpi_batch_records(rows: list[dict], cooling_power_ratio: float) -> list[dict]:
    if not rows:
        return []

    df = pd.DataFrame([dict(r) for r in rows])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).copy()
    if df.empty:
        return []

    # Ensure rolling windows are computed on deterministic per-miner chronology.
    df = df.sort_values(["miner_id", "timestamp"], kind="mergesort").reset_index(drop=True)

    df["cooling_power_w"] = (
        pd.to_numeric(df["asic_power_w"], errors="coerce").fillna(0.0)
        * cooling_power_ratio
    )
    df["efficiency_j_per_th"] = (
        df["asic_power_w"] / df["asic_hashrate_ths"].replace(0, np.nan)
    ).round(4)

    # Power instability index: rolling coefficient of variation per miner.
    asic_power = pd.to_numeric(df["asic_power_w"], errors="coerce")
    rolling_mean = asic_power.groupby(df["miner_id"]).transform(
        lambda series: series.rolling(
            window=POWER_INSTABILITY_WINDOW, min_periods=2
        ).mean()
    )
    rolling_std = asic_power.groupby(df["miner_id"]).transform(
        lambda series: series.rolling(
            window=POWER_INSTABILITY_WINDOW, min_periods=2
        ).std(ddof=0)
    )
    df["power_instability_index"] = (
        (rolling_std / rolling_mean.replace(0, np.nan))
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
        .clip(0, 1)
        .round(4)
    )

    # Hashrate deviation vs mode median.
    mode_medians = df.groupby("operating_mode")["asic_hashrate_ths"].transform("median")
    df["hashrate_deviation_pct"] = (
        (df["asic_hashrate_ths"] - mode_medians)
        / mode_medians.replace(0, np.nan)
        * 100
    ).round(2)

    df["true_efficiency_te"] = _compute_te(df)
    df["failure_within_horizon"] = 0  # Updated during retraining.

    insert_cols = [
        "timestamp", "miner_id", "asic_clock_mhz", "asic_voltage_v",
        "asic_hashrate_ths", "asic_temperature_c", "asic_power_w",
        "operating_mode", "ambient_temperature_c", "efficiency_j_per_th",
        "power_instability_index", "hashrate_deviation_pct",
        "true_efficiency_te", "failure_within_horizon",
        "chip_temp_max", "chip_temp_std", "bad_hash_count",
        "double_hash_count", "read_errors", "event_codes",
        "expected_hashrate_ths",
    ]
    return _build_kpi_insert_records(df, insert_cols)


# ── KPI Job ────────────────────────────────────────────────────────────────
def run_kpi_job(engine: Engine):
    runtime_cfg = _load_runtime_config(engine)
    cooling_power_ratio = float(runtime_cfg.get("cooling_power_ratio", COOLING_POWER_RATIO))

    last_seen_id = 0
    rows_scanned = 0
    rows_inserted = 0

    while rows_scanned < KPI_JOB_MAX_ROWS_PER_RUN:
        batch_limit = min(KPI_JOB_BATCH_SIZE, KPI_JOB_MAX_ROWS_PER_RUN - rows_scanned)
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT t.id, t.timestamp, t.miner_id, t.asic_clock_mhz,
                           t.asic_voltage_v, t.asic_hashrate_ths, t.asic_temperature_c,
                           t.asic_power_w, t.operating_mode, t.ambient_temperature_c,
                           t.chip_temp_max, t.chip_temp_std, t.bad_hash_count,
                           t.double_hash_count, t.read_errors, t.event_codes,
                           t.expected_hashrate_ths
                    FROM telemetry t
                    WHERE t.id > :last_seen_id
                      AND NOT EXISTS (
                          SELECT 1 FROM kpi_telemetry k
                          WHERE k.miner_id = t.miner_id AND k.timestamp = t.timestamp
                      )
                    ORDER BY t.id ASC
                    LIMIT :lim
                """),
                {"last_seen_id": int(last_seen_id), "lim": int(batch_limit)},
            )
            rows = result.mappings().all()

        if not rows:
            break

        rows_scanned += len(rows)
        last_seen_id = int(rows[-1]["id"])
        records = _prepare_kpi_batch_records(rows, cooling_power_ratio)
        if records:
            with engine.begin() as conn:
                conn.execute(_KPI_INSERT_SQL, records)
            rows_inserted += len(records)

        if len(rows) < batch_limit:
            break

    if rows_scanned == 0:
        logger.info("KPI job: no new telemetry rows to process")
        return
    logger.info(
        "KPI job: inserted %d rows from %d scanned telemetry rows",
        rows_inserted,
        rows_scanned,
    )


# ── Inference Job ──────────────────────────────────────────────────────────
def run_inference_job(engine: Engine):
    runtime_cfg = _load_runtime_config(engine)
    threshold = float(runtime_cfg["risk_threshold"])
    cooldown_hours = int(runtime_cfg["alert_cooldown_hours"])
    lookback_hours = int(runtime_cfg["inference_lookback_hours"])
    cooling_power_ratio = float(runtime_cfg["cooling_power_ratio"])

    if not MODEL_PATH.exists():
        logger.warning("Inference: no model found at %s — skipping", MODEL_PATH)
        _run_heuristic_inference(engine, threshold, cooldown_hours, runtime_cfg)
        return

    try:
        artifact = joblib.load(MODEL_PATH)
        pipeline = artifact["pipeline"]
        feature_cols = artifact["feature_columns"]
        serving_defaults = artifact.get("serving_defaults", {})
        if isinstance(serving_defaults, dict):
            cooling_power_ratio = max(
                float(serving_defaults.get("cooling_power_ratio", cooling_power_ratio)),
                0.0,
            )
    except Exception as exc:
        logger.error(
            "Inference: failed to load model artifact (%s), falling back to heuristic",
            exc,
        )
        _run_heuristic_inference(engine, threshold, cooldown_hours, runtime_cfg)
        return

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                miner_id, timestamp, asic_clock_mhz, asic_voltage_v,
                asic_hashrate_ths, asic_temperature_c, asic_power_w,
                operating_mode, ambient_temperature_c,
                true_efficiency_te, efficiency_j_per_th,
                power_instability_index, hashrate_deviation_pct,
                chip_temp_max, expected_hashrate_ths,
                event_codes, failure_within_horizon
            FROM kpi_telemetry
            WHERE timestamp >= NOW() - (CAST(:lookback_hours AS text) || ' hours')::interval
            ORDER BY miner_id, timestamp ASC
        """), {"lookback_hours": lookback_hours})
        rows = result.mappings().all()

    if not rows:
        logger.info("Inference: no KPI data yet")
        return

    history_df = pd.DataFrame([dict(r) for r in rows])
    history_df["timestamp"] = pd.to_datetime(history_df["timestamp"], utc=True, errors="coerce")
    history_df = history_df.dropna(subset=["timestamp"]).copy()
    if history_df.empty:
        logger.info("Inference: no valid timestamped KPI rows in lookback window")
        return

    from src.feature_engineering import build_serving_feature_snapshot

    feature_df = build_serving_feature_snapshot(
        history_df,
        feature_cols=feature_cols,
        cooling_power_ratio=cooling_power_ratio,
    )
    if feature_df.empty:
        logger.info("Inference: feature snapshot is empty")
        return

    latest_context = (
        history_df.sort_values(["miner_id", "timestamp"])
        .groupby("miner_id", as_index=False, group_keys=False)
        .tail(1)[["miner_id", "timestamp", "event_codes"]]
        .reset_index(drop=True)
    )
    df = feature_df.merge(latest_context, on=["miner_id", "timestamp"], how="left")
    if "event_codes" not in df.columns:
        df["event_codes"] = None

    X = _prepare_inference_features(df, feature_cols)

    try:
        scores = pipeline.predict_proba(X)[:, 1]
    except Exception as exc:
        logger.error(
            "Inference: model scoring failed (%s), falling back to heuristic", exc
        )
        _run_heuristic_inference(engine, threshold, cooldown_hours, runtime_cfg)
        return

    df["risk_score"] = scores
    df["risk_band"] = [_risk_band(s) for s in scores]
    df["predicted_failure"] = scores >= threshold

    _write_predictions(engine, df)
    _generate_alerts(engine, df, threshold, cooldown_hours, runtime_cfg)
    logger.info(
        "Inference: scored %d miners (lookback=%dh, cooling_ratio=%.4f)",
        len(df),
        lookback_hours,
        cooling_power_ratio,
    )


def _run_heuristic_inference(
    engine: Engine,
    threshold: float,
    cooldown_hours: int,
    runtime_cfg: dict | None = None,
):
    """Simple threshold-based scoring when no ML model is trained yet."""
    if runtime_cfg is None:
        runtime_cfg = _load_runtime_config(engine)
    with engine.connect() as conn:
        result = conn.execute(text("""
            WITH miners AS (
                SELECT DISTINCT miner_id
                FROM kpi_telemetry
            )
            SELECT
                latest.miner_id,
                latest.asic_temperature_c,
                latest.asic_power_w,
                latest.asic_hashrate_ths,
                latest.power_instability_index,
                latest.event_codes
            FROM miners m
            JOIN LATERAL (
                SELECT
                    k.miner_id,
                    k.asic_temperature_c,
                    k.asic_power_w,
                    k.asic_hashrate_ths,
                    k.power_instability_index,
                    k.event_codes
                FROM kpi_telemetry k
                WHERE k.miner_id = m.miner_id
                ORDER BY k.timestamp DESC
                LIMIT 1
            ) latest ON TRUE
        """))
        rows = result.mappings().all()

    if not rows:
        return

    df = pd.DataFrame([dict(r) for r in rows])
    # Heuristic score: normalised weighted combo of risk factors
    temp_score = np.clip((df["asic_temperature_c"].fillna(70) - 60) / 40, 0, 1)
    hash_penalty = np.where(df["asic_hashrate_ths"].fillna(100) < 50, 1.0, 
                   np.where(df["asic_hashrate_ths"].fillna(100) < 80, 0.5, 0.0))
    instab_score = df["power_instability_index"].fillna(0).clip(0, 1)
    scores = (0.5 * temp_score + 0.4 * hash_penalty + 0.1 * instab_score).clip(0, 1)

    df["risk_score"] = scores.round(4)
    df["risk_band"] = [_risk_band(s) for s in scores]
    df["predicted_failure"] = scores >= threshold
    _write_predictions(engine, df)
    _generate_alerts(engine, df, threshold, cooldown_hours, runtime_cfg)
    logger.info("Heuristic inference: scored %d miners", len(df))


def _write_predictions(engine: Engine, df: pd.DataFrame):
    records = df[
        ["miner_id", "risk_score", "risk_band", "predicted_failure"]
    ].to_dict(orient="records")
    if not records:
        return
    for row in records:
        row["model_version"] = MODEL_VERSION

    upsert_sql = text("""
        INSERT INTO risk_predictions (
            predicted_at, miner_id, risk_score, risk_band, predicted_failure, model_version
        )
        VALUES (
            NOW(), :miner_id, :risk_score, :risk_band, :predicted_failure, :model_version
        )
        ON CONFLICT (miner_id, model_version)
        DO UPDATE SET
            predicted_at = EXCLUDED.predicted_at,
            risk_score = EXCLUDED.risk_score,
            risk_band = EXCLUDED.risk_band,
            predicted_failure = EXCLUDED.predicted_failure
    """)
    fallback_insert_sql = text("""
        INSERT INTO risk_predictions (
            predicted_at, miner_id, risk_score, risk_band, predicted_failure, model_version
        )
        VALUES (
            NOW(), :miner_id, :risk_score, :risk_band, :predicted_failure, :model_version
        )
    """)

    with engine.begin() as conn:
        try:
            _ensure_risk_predictions_upsert(conn)
            conn.execute(upsert_sql, records)
        except Exception as exc:
            logger.warning(
                "Risk prediction upsert unavailable; using bounded replace fallback: %s",
                exc,
            )
            miner_ids = sorted(
                {
                    str(row["miner_id"]).strip()
                    for row in records
                    if str(row.get("miner_id", "")).strip()
                }
            )
            if miner_ids:
                conn.execute(
                    text("""
                        DELETE FROM risk_predictions
                        WHERE miner_id = ANY(CAST(:miner_ids AS text[]))
                          AND model_version = :model_version
                    """),
                    {"miner_ids": miner_ids, "model_version": MODEL_VERSION},
                )
            conn.execute(fallback_insert_sql, records)


def _ensure_risk_predictions_upsert(conn) -> None:
    global _RISK_PREDICTIONS_UPSERT_READY
    if _RISK_PREDICTIONS_UPSERT_READY:
        return
    with _RISK_PREDICTIONS_UPSERT_LOCK:
        if _RISK_PREDICTIONS_UPSERT_READY:
            return

        conn.execute(
            text(
                "UPDATE risk_predictions SET model_version = 'v1' WHERE model_version IS NULL"
            )
        )
        conn.execute(
            text("""
                DELETE FROM risk_predictions rp
                USING (
                    SELECT id
                    FROM (
                        SELECT
                            id,
                            ROW_NUMBER() OVER (
                                PARTITION BY miner_id, model_version
                                ORDER BY predicted_at DESC, id DESC
                            ) AS rn
                        FROM risk_predictions
                    ) ranked
                    WHERE ranked.rn > 1
                ) dupes
                WHERE rp.id = dupes.id
            """)
        )
        conn.execute(
            text("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_risk_predictions_miner_model
                ON risk_predictions (miner_id, model_version)
            """)
        )
        _RISK_PREDICTIONS_UPSERT_READY = True


def _generate_alerts(
    engine: Engine,
    df: pd.DataFrame,
    threshold: float,
    cooldown_hours: int,
    runtime_cfg: dict,
):
    high_risk = df[df["risk_score"] >= threshold].copy()
    if high_risk.empty:
        return

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT DISTINCT miner_id FROM alerts
            WHERE resolved = FALSE
              AND created_at >= NOW() - (CAST(:cooldown_hours AS text) || ' hours')::interval
        """), {"cooldown_hours": int(cooldown_hours)})
        cooldown_miners = {r[0] for r in result}

    new_alerts = high_risk[~high_risk["miner_id"].isin(cooldown_miners)]
    if new_alerts.empty:
        return

    policy_cfg = runtime_cfg.get("policy") or parse_policy_config(runtime_cfg)
    backtest_summary = backtest_policy_uplift(
        rows=[dict(row) for row in df.to_dict(orient="records")],
        cfg=policy_cfg,
    )
    policy_report = {
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "threshold": threshold,
        "cooldown_hours": cooldown_hours,
        "policy": policy_cfg,
        "backtest": backtest_summary,
    }
    _write_policy_backtest_report(policy_report)

    optimizer_enabled = bool(policy_cfg.get("optimizer_enabled", True))
    require_backtest = bool(policy_cfg.get("require_backtest_for_automation", True))
    backtest_passed = bool(backtest_summary.get("passed", True))
    control_mode = str(runtime_cfg.get("control_mode", "advisory")).strip().lower()
    control_mode_allows_automation = control_mode == "actuation"

    records = []
    for _, row in new_alerts.iterrows():
        row_dict = row.to_dict()
        score = float(row["risk_score"])
        severity = "critical" if score >= 0.75 else "warning"

        if optimizer_enabled:
            decision = optimize_policy_decision(row_dict, policy_cfg)
            policy_name = "optimized"
        else:
            decision = baseline_policy_decision(row_dict)
            policy_name = "baseline"

        action = decision["action"]
        requested_automation = bool(decision["automation_triggered"])
        gate_automation_backtest = (
            require_backtest and (not backtest_passed) and requested_automation
        )
        gate_automation_mode = requested_automation and (not control_mode_allows_automation)
        gate_automation = gate_automation_backtest or gate_automation_mode
        automation = requested_automation and (not gate_automation)

        utility = decision.get("expected_utility_usd")
        utility_note = (
            f", utility=${float(utility):.2f}" if utility is not None else ""
        )
        gate_notes = []
        if gate_automation_backtest:
            gate_notes.append("automation_gated_by_backtest")
        if gate_automation_mode:
            gate_notes.append(f"automation_gated_by_mode:{control_mode}")
        gate_note = f"; {'|'.join(gate_notes)}" if gate_notes else ""
        decision_reason = str(decision.get("reason", "decision_unavailable"))

        records.append({
            "miner_id": row["miner_id"],
            "severity": severity,
            "risk_score": round(score, 4),
            "trigger_reason": (
                f"Risk score {score:.2%} exceeds threshold; "
                f"policy={policy_name}; reason={decision_reason}{utility_note}{gate_note}"
            ),
            "message": (
                f"Miner {row['miner_id']} — {severity.upper()}: predicted failure risk "
                f"{score:.2%}. Action={action} ({policy_name}){utility_note}{gate_note}"
            ),
            "recommended_action": action,
            "automation_triggered": automation,
        })

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO alerts (miner_id, severity, risk_score, trigger_reason, message, recommended_action, automation_triggered)
                VALUES (:miner_id, :severity, :risk_score, :trigger_reason, :message, :recommended_action, :automation_triggered)
            """),
            records,
        )
    logger.info(
        "Generated %d new alerts (policy=%s, backtest_passed=%s, control_mode=%s, avg_uplift=%.4f)",
        len(records),
        "optimized" if optimizer_enabled else "baseline",
        backtest_passed,
        control_mode,
        float(backtest_summary.get("avg_uplift_usd_per_miner", 0.0)),
    )


# ── Retrain Job ────────────────────────────────────────────────────────────
def run_retrain_job(engine: Engine):
    runtime_cfg = _load_runtime_config(engine)
    retrain_days = int(runtime_cfg.get("retrain_days", RETRAIN_DAYS))
    cooling_power_ratio = max(
        float(runtime_cfg.get("cooling_power_ratio", COOLING_POWER_RATIO)),
        0.0,
    )
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT timestamp, miner_id, asic_clock_mhz, asic_voltage_v,
                   asic_hashrate_ths, asic_temperature_c, asic_power_w,
                   operating_mode, true_efficiency_te, efficiency_j_per_th,
                   power_instability_index, hashrate_deviation_pct,
                   ambient_temperature_c,
                   asic_power_w * :cooling_power_ratio AS cooling_power_w,
                   failure_within_horizon,
                   chip_temp_max, expected_hashrate_ths
            FROM kpi_telemetry
            WHERE timestamp >= NOW() - (CAST(:retrain_days AS text) || ' days')::interval
            ORDER BY timestamp ASC
        """), {"cooling_power_ratio": cooling_power_ratio, "retrain_days": retrain_days})
        rows = result.mappings().all()

    if len(rows) < RETRAIN_MIN_ROWS:
        logger.warning(
            "Retrain: not enough data (%d rows < min_rows=%d), skipping",
            len(rows),
            RETRAIN_MIN_ROWS,
        )
        return

    df = pd.DataFrame([dict(r) for r in rows])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).copy()
    if len(df) < RETRAIN_MIN_ROWS:
        logger.warning(
            "Retrain: not enough valid timestamped data (%d rows < min_rows=%d), skipping",
            len(df),
            RETRAIN_MIN_ROWS,
        )
        return

    miner_count = int(df["miner_id"].nunique(dropna=True))
    if miner_count < RETRAIN_MIN_MINERS:
        logger.warning(
            "Retrain: not enough miner diversity (%d miners < min_miners=%d), skipping",
            miner_count,
            RETRAIN_MIN_MINERS,
        )
        return

    timespan_hours = 0.0
    if len(df) > 1:
        delta = df["timestamp"].max() - df["timestamp"].min()
        timespan_hours = max(delta.total_seconds() / 3600.0, 0.0)
    if timespan_hours < RETRAIN_MIN_TIMESPAN_HOURS:
        logger.warning(
            "Retrain: insufficient time span (%.2fh < min_hours=%.2f), skipping",
            timespan_hours,
            RETRAIN_MIN_TIMESPAN_HOURS,
        )
        return

    logger.info(
        "Retrain: using %d rows across %d miners over %.2f hours",
        len(df),
        miner_count,
        timespan_hours,
    )

    try:
        from src.feature_engineering import run_feature_engineering
        from src.train import run_training_pipeline

        features_df, feature_cols, _, _ = run_feature_engineering(df)
        MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
        run_training_pipeline(
            features_df,
            feature_cols,
            model_artifact_path=MODEL_PATH,
        )
        logger.info("Retrain: model saved to %s", MODEL_PATH)
    except Exception as exc:
        logger.error("Retrain failed: %s", exc)
