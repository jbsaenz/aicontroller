"""CSV upload and external API source management endpoints."""

import io
import json
import logging
import os
import random
from datetime import datetime, timedelta, timezone

import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import verify_token
from api.db import get_db
from api.schemas import ApiSourceIn, ApiSourceOut, IngestResult
from src.url_safety import (
    UnsafeURLError,
    get_source_allowlist,
    inspect_source_url,
    validate_source_url,
)

logger = logging.getLogger(__name__)
router = APIRouter()

MAX_INGEST_FILE_BYTES = int(os.getenv("MAX_INGEST_FILE_BYTES", "10485760"))  # 10 MiB
MAX_INGEST_ROWS = int(os.getenv("MAX_INGEST_ROWS", "200000"))
INGEST_READ_CHUNK_BYTES = int(os.getenv("INGEST_READ_CHUNK_BYTES", "1048576"))
DEMO_SEED_MINERS = int(os.getenv("DEMO_SEED_MINERS", "120"))
DEMO_SEED_HISTORY_POINTS = int(os.getenv("DEMO_SEED_HISTORY_POINTS", "4"))
DEMO_SEED_MODEL_VERSION = (
    os.getenv("DEMO_SEED_MODEL_VERSION", "demo-seed-v1").strip() or "demo-seed-v1"
)
DEMO_COOLING_POWER_RATIO = 0.24
DEMO_MODE_FACTORS = {"eco": 0.97, "normal": 1.00, "turbo": 1.08}

REQUIRED_COLS = {"timestamp", "miner_id"}
ALLOWED_COLS = {
    "timestamp", "miner_id", "asic_clock_mhz", "asic_voltage_v",
    "asic_hashrate_ths", "asic_temperature_c", "asic_power_w",
    "operating_mode", "ambient_temperature_c",
    "chip_temp_max", "chip_temp_std", "bad_hash_count",
    "double_hash_count", "read_errors", "event_codes",
    "expected_hashrate_ths"
}
TEXT_COLS = {"miner_id", "operating_mode", "event_codes"}
TELEMETRY_JSON_COL_SPECS = [
    ("timestamp", "timestamptz"),
    ("miner_id", "text"),
    ("asic_clock_mhz", "double precision"),
    ("asic_voltage_v", "double precision"),
    ("asic_hashrate_ths", "double precision"),
    ("asic_temperature_c", "double precision"),
    ("asic_power_w", "double precision"),
    ("operating_mode", "text"),
    ("ambient_temperature_c", "double precision"),
    ("chip_temp_max", "double precision"),
    ("chip_temp_std", "double precision"),
    ("bad_hash_count", "integer"),
    ("double_hash_count", "integer"),
    ("read_errors", "integer"),
    ("event_codes", "text"),
    ("expected_hashrate_ths", "double precision"),
]
TELEMETRY_INSERT_COLS = [col for col, _ in TELEMETRY_JSON_COL_SPECS]
INGEST_JSON_INSERT_SQL = text(
    """
    WITH payload AS (
        SELECT
            timestamp, miner_id, asic_clock_mhz, asic_voltage_v,
            asic_hashrate_ths, asic_temperature_c, asic_power_w,
            operating_mode, ambient_temperature_c, chip_temp_max,
            chip_temp_std, bad_hash_count, double_hash_count,
            read_errors, event_codes, expected_hashrate_ths
        FROM jsonb_to_recordset(CAST(:payload AS jsonb)) AS src(
            timestamp timestamptz,
            miner_id text,
            asic_clock_mhz double precision,
            asic_voltage_v double precision,
            asic_hashrate_ths double precision,
            asic_temperature_c double precision,
            asic_power_w double precision,
            operating_mode text,
            ambient_temperature_c double precision,
            chip_temp_max double precision,
            chip_temp_std double precision,
            bad_hash_count integer,
            double_hash_count integer,
            read_errors integer,
            event_codes text,
            expected_hashrate_ths double precision
        )
    ),
    inserted AS (
        INSERT INTO telemetry (
            timestamp, miner_id, asic_clock_mhz, asic_voltage_v,
            asic_hashrate_ths, asic_temperature_c, asic_power_w,
            operating_mode, ambient_temperature_c, chip_temp_max,
            chip_temp_std, bad_hash_count, double_hash_count,
            read_errors, event_codes, expected_hashrate_ths, source
        )
        SELECT
            timestamp, miner_id, asic_clock_mhz, asic_voltage_v,
            asic_hashrate_ths, asic_temperature_c, asic_power_w,
            operating_mode, ambient_temperature_c, chip_temp_max,
            chip_temp_std, bad_hash_count, double_hash_count,
            read_errors, event_codes, expected_hashrate_ths, 'csv'
        FROM payload
        ON CONFLICT (miner_id, timestamp) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*)::int AS inserted_rows
    FROM inserted
    """
)

KPI_JSON_COL_SPECS = [
    ("timestamp", "timestamptz"),
    ("miner_id", "text"),
    ("asic_clock_mhz", "double precision"),
    ("asic_voltage_v", "double precision"),
    ("asic_hashrate_ths", "double precision"),
    ("asic_temperature_c", "double precision"),
    ("asic_power_w", "double precision"),
    ("operating_mode", "text"),
    ("ambient_temperature_c", "double precision"),
    ("efficiency_j_per_th", "double precision"),
    ("power_instability_index", "double precision"),
    ("hashrate_deviation_pct", "double precision"),
    ("true_efficiency_te", "double precision"),
    ("failure_within_horizon", "integer"),
    ("chip_temp_max", "double precision"),
    ("chip_temp_std", "double precision"),
    ("bad_hash_count", "integer"),
    ("double_hash_count", "integer"),
    ("read_errors", "integer"),
    ("event_codes", "text"),
    ("expected_hashrate_ths", "double precision"),
]
KPI_INSERT_COLS = [col for col, _ in KPI_JSON_COL_SPECS]
KPI_JSON_INSERT_SQL = text(
    """
    WITH payload AS (
        SELECT
            timestamp, miner_id, asic_clock_mhz, asic_voltage_v,
            asic_hashrate_ths, asic_temperature_c, asic_power_w,
            operating_mode, ambient_temperature_c, efficiency_j_per_th,
            power_instability_index, hashrate_deviation_pct, true_efficiency_te,
            failure_within_horizon, chip_temp_max, chip_temp_std, bad_hash_count,
            double_hash_count, read_errors, event_codes, expected_hashrate_ths
        FROM jsonb_to_recordset(CAST(:payload AS jsonb)) AS src(
            timestamp timestamptz,
            miner_id text,
            asic_clock_mhz double precision,
            asic_voltage_v double precision,
            asic_hashrate_ths double precision,
            asic_temperature_c double precision,
            asic_power_w double precision,
            operating_mode text,
            ambient_temperature_c double precision,
            efficiency_j_per_th double precision,
            power_instability_index double precision,
            hashrate_deviation_pct double precision,
            true_efficiency_te double precision,
            failure_within_horizon integer,
            chip_temp_max double precision,
            chip_temp_std double precision,
            bad_hash_count integer,
            double_hash_count integer,
            read_errors integer,
            event_codes text,
            expected_hashrate_ths double precision
        )
    ),
    inserted AS (
        INSERT INTO kpi_telemetry (
            timestamp, miner_id, asic_clock_mhz, asic_voltage_v,
            asic_hashrate_ths, asic_temperature_c, asic_power_w,
            operating_mode, ambient_temperature_c, efficiency_j_per_th,
            power_instability_index, hashrate_deviation_pct, true_efficiency_te,
            failure_within_horizon, chip_temp_max, chip_temp_std, bad_hash_count,
            double_hash_count, read_errors, event_codes, expected_hashrate_ths
        )
        SELECT
            timestamp, miner_id, asic_clock_mhz, asic_voltage_v,
            asic_hashrate_ths, asic_temperature_c, asic_power_w,
            operating_mode, ambient_temperature_c, efficiency_j_per_th,
            power_instability_index, hashrate_deviation_pct, true_efficiency_te,
            failure_within_horizon, chip_temp_max, chip_temp_std, bad_hash_count,
            double_hash_count, read_errors, event_codes, expected_hashrate_ths
        FROM payload
        ON CONFLICT (miner_id, timestamp) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*)::int AS inserted_rows
    FROM inserted
    """
)

RISK_JSON_COL_SPECS = [
    ("predicted_at", "timestamptz"),
    ("miner_id", "text"),
    ("risk_score", "double precision"),
    ("risk_band", "text"),
    ("predicted_failure", "boolean"),
    ("model_version", "text"),
]
RISK_INSERT_COLS = [col for col, _ in RISK_JSON_COL_SPECS]
RISK_JSON_UPSERT_SQL = text(
    """
    WITH payload AS (
        SELECT
            predicted_at, miner_id, risk_score, risk_band, predicted_failure, model_version
        FROM jsonb_to_recordset(CAST(:payload AS jsonb)) AS src(
            predicted_at timestamptz,
            miner_id text,
            risk_score double precision,
            risk_band text,
            predicted_failure boolean,
            model_version text
        )
    ),
    upserted AS (
        INSERT INTO risk_predictions (
            predicted_at, miner_id, risk_score, risk_band, predicted_failure, model_version
        )
        SELECT
            predicted_at, miner_id, risk_score, risk_band, predicted_failure, model_version
        FROM payload
        ON CONFLICT (miner_id, model_version) DO UPDATE
        SET
            predicted_at = EXCLUDED.predicted_at,
            risk_score = EXCLUDED.risk_score,
            risk_band = EXCLUDED.risk_band,
            predicted_failure = EXCLUDED.predicted_failure
        RETURNING 1
    )
    SELECT COUNT(*)::int AS upserted_rows
    FROM upserted
    """
)


class SourceUrlValidationIn(BaseModel):
    url_template: str


class DemoSeedResult(BaseModel):
    rows_received: int
    rows_inserted: int
    kpi_rows_inserted: int
    risk_rows_inserted: int
    miners_found: list[str]
    errors: list[str] = []


def _nullify_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in TEXT_COLS:
        if col not in df.columns:
            continue
        df[col] = df[col].map(
            lambda value: None if pd.isna(value) else str(value).strip()
        )
        if col != "miner_id":
            df.loc[df[col] == "", col] = None
    return df


def _validate_and_clean(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    errors: list[str] = []
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise HTTPException(400, f"CSV missing required columns: {missing}")

    df = df[[c for c in df.columns if c in ALLOWED_COLS]].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    bad_ts = df["timestamp"].isna().sum()
    if bad_ts:
        errors.append(f"Dropped {bad_ts} rows with unparseable timestamps")
        df = df.dropna(subset=["timestamp"])

    for col in ALLOWED_COLS - {"timestamp", "miner_id", "operating_mode", "event_codes"}:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "operating_mode" not in df.columns:
        df["operating_mode"] = "normal"

    df = _nullify_text_columns(df)
    if "miner_id" in df.columns:
        missing_miner = df["miner_id"].isna() | (df["miner_id"] == "")
        missing_miner_count = int(missing_miner.sum())
        if missing_miner_count:
            errors.append(f"Dropped {missing_miner_count} rows with missing miner_id")
            df = df.loc[~missing_miner]

    df = df.drop_duplicates(subset=["miner_id", "timestamp"])
    return df, errors


def _build_json_payload(df: pd.DataFrame, columns: list[str]) -> str:
    records = []
    for row in df.reindex(columns=columns).to_dict(orient="records"):
        cleaned = {}
        for key, value in row.items():
            if isinstance(value, pd.Timestamp):
                cleaned[key] = value.isoformat()
                continue
            try:
                cleaned[key] = None if pd.isna(value) else value
            except TypeError:
                cleaned[key] = value
        records.append(cleaned)
    return json.dumps(records, allow_nan=False)


def _build_ingest_payload(df: pd.DataFrame) -> str:
    return _build_json_payload(df, TELEMETRY_INSERT_COLS)


def _build_kpi_payload(df: pd.DataFrame) -> str:
    return _build_json_payload(df, KPI_INSERT_COLS)


def _build_risk_payload(df: pd.DataFrame) -> str:
    return _build_json_payload(df, RISK_INSERT_COLS)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _risk_band(score: float) -> str:
    if score >= 0.75:
        return "critical"
    if score >= 0.55:
        return "high"
    if score >= 0.35:
        return "medium"
    return "low"


def _compute_true_efficiency(
    asic_hashrate_ths: float,
    asic_power_w: float,
    asic_voltage_v: float,
    ambient_temperature_c: float,
    operating_mode: str,
) -> float:
    cooling_power_w = asic_power_w * DEMO_COOLING_POWER_RATIO
    total_power = max(asic_power_w + cooling_power_w, 1e-6)
    base_eff = asic_hashrate_ths / total_power
    voltage_stress = 1.0 + 0.6 * max(0.0, (asic_voltage_v - 12.5) / 12.5)
    env_stress = 1.0 + 0.4 * max(0.0, (ambient_temperature_c - 25.0) / 10.0)
    mode_factor = DEMO_MODE_FACTORS.get(operating_mode, 1.0)
    return round(base_eff / max(voltage_stress * env_stress * mode_factor, 1e-6), 6)


def _generate_demo_seed_frames(
    miners: int,
    history_points: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    miners = max(20, min(int(miners), 1000))
    history_points = max(3, min(int(history_points), 8))
    now = datetime.now(timezone.utc).replace(microsecond=0)
    run_token = now.strftime("%Y%m%d%H%M%S%f")
    rng = random.Random(now.timestamp())

    healthy_count = max(1, int(miners * 0.72))
    medium_count = max(1, int(miners * 0.16))
    high_count = max(1, int(miners * 0.08))
    critical_count = max(1, miners - healthy_count - medium_count - high_count)

    profiles = [
        {
            "prefix": "HLTHY",
            "count": healthy_count,
            "mode": "normal",
            "temp": (58.0, 70.0),
            "hashrate": (98.0, 118.0),
            "voltage": (12.0, 12.5),
            "power": (2850.0, 3120.0),
            "instability": (0.06, 0.16),
            "drift": 0.04,
            "failure_prob": 0.01,
        },
        {
            "prefix": "MED",
            "count": medium_count,
            "mode": "normal",
            "temp": (74.0, 84.0),
            "hashrate": (86.0, 98.0),
            "voltage": (12.4, 13.0),
            "power": (3080.0, 3360.0),
            "instability": (0.22, 0.38),
            "drift": 0.12,
            "failure_prob": 0.08,
        },
        {
            "prefix": "HIGH",
            "count": high_count,
            "mode": "turbo",
            "temp": (86.0, 97.0),
            "hashrate": (68.0, 84.0),
            "voltage": (13.0, 13.6),
            "power": (3340.0, 3620.0),
            "instability": (0.45, 0.67),
            "drift": 0.20,
            "failure_prob": 0.18,
        },
        {
            "prefix": "CRIT",
            "count": critical_count,
            "mode": "turbo",
            "temp": (98.0, 114.0),
            "hashrate": (22.0, 52.0),
            "voltage": (13.6, 14.5),
            "power": (3600.0, 3920.0),
            "instability": (0.70, 0.93),
            "drift": 0.30,
            "failure_prob": 0.42,
        },
    ]

    telemetry_rows: list[dict] = []
    kpi_rows: list[dict] = []
    risk_rows: list[dict] = []
    for profile in profiles:
        for idx in range(profile["count"]):
            miner_id = f"{profile['prefix']}-{run_token}-{idx + 1:04d}"
            mode = profile["mode"]
            expected_hashrate = rng.uniform(profile["hashrate"][1], profile["hashrate"][1] + 8.0)
            latest_snapshot: dict[str, float] = {}
            for step in range(history_points - 1, -1, -1):
                drift = (history_points - 1 - step) / max(history_points - 1, 1)
                timestamp = now - timedelta(hours=step)
                asic_clock_mhz = rng.uniform(382.0, 430.0)
                asic_voltage_v = rng.uniform(*profile["voltage"]) + profile["drift"] * drift
                asic_temperature_c = rng.uniform(*profile["temp"]) + (profile["drift"] * 20.0 * drift)
                asic_power_w = rng.uniform(*profile["power"]) + (profile["drift"] * 300.0 * drift)
                asic_hashrate_ths = max(
                    5.0,
                    rng.uniform(*profile["hashrate"]) * (1.0 - profile["drift"] * drift),
                )
                ambient_temperature_c = rng.uniform(22.0, 38.0)

                power_instability_index = _clamp(
                    rng.uniform(*profile["instability"]) + 0.08 * drift,
                    0.0,
                    1.0,
                )
                hashrate_deviation_pct = (
                    (asic_hashrate_ths - expected_hashrate) / max(expected_hashrate, 1e-6)
                ) * 100.0
                efficiency_j_per_th = asic_power_w / max(asic_hashrate_ths, 1e-6)
                true_efficiency_te = _compute_true_efficiency(
                    asic_hashrate_ths=asic_hashrate_ths,
                    asic_power_w=asic_power_w,
                    asic_voltage_v=asic_voltage_v,
                    ambient_temperature_c=ambient_temperature_c,
                    operating_mode=mode,
                )
                chip_temp_max = asic_temperature_c + rng.uniform(0.8, 4.8)
                chip_temp_std = rng.uniform(1.2, 8.4)
                bad_hash_count = int(max(0, round((power_instability_index * 120) + rng.uniform(0, 10))))
                double_hash_count = int(max(0, round((power_instability_index * 260) + rng.uniform(0, 15))))
                read_errors = int(max(0, round((power_instability_index * 12) + rng.uniform(0, 3))))
                failure_within_horizon = int(rng.random() < (profile["failure_prob"] + 0.10 * drift))

                event_codes = None
                if profile["prefix"] in {"HIGH", "CRIT"} and drift > 0.66:
                    event_codes = "[\"LOW_HASHRATE\",\"HIGH_TEMP\"]"

                telemetry_rows.append(
                    {
                        "timestamp": timestamp,
                        "miner_id": miner_id,
                        "asic_clock_mhz": round(asic_clock_mhz, 3),
                        "asic_voltage_v": round(asic_voltage_v, 4),
                        "asic_hashrate_ths": round(asic_hashrate_ths, 4),
                        "asic_temperature_c": round(asic_temperature_c, 4),
                        "asic_power_w": round(asic_power_w, 4),
                        "operating_mode": mode,
                        "ambient_temperature_c": round(ambient_temperature_c, 4),
                        "chip_temp_max": round(chip_temp_max, 4),
                        "chip_temp_std": round(chip_temp_std, 4),
                        "bad_hash_count": bad_hash_count,
                        "double_hash_count": double_hash_count,
                        "read_errors": read_errors,
                        "event_codes": event_codes,
                        "expected_hashrate_ths": round(expected_hashrate, 4),
                    }
                )

                kpi_rows.append(
                    {
                        "timestamp": timestamp,
                        "miner_id": miner_id,
                        "asic_clock_mhz": round(asic_clock_mhz, 3),
                        "asic_voltage_v": round(asic_voltage_v, 4),
                        "asic_hashrate_ths": round(asic_hashrate_ths, 4),
                        "asic_temperature_c": round(asic_temperature_c, 4),
                        "asic_power_w": round(asic_power_w, 4),
                        "operating_mode": mode,
                        "ambient_temperature_c": round(ambient_temperature_c, 4),
                        "efficiency_j_per_th": round(efficiency_j_per_th, 6),
                        "power_instability_index": round(power_instability_index, 6),
                        "hashrate_deviation_pct": round(hashrate_deviation_pct, 6),
                        "true_efficiency_te": round(true_efficiency_te, 6),
                        "failure_within_horizon": failure_within_horizon,
                        "chip_temp_max": round(chip_temp_max, 4),
                        "chip_temp_std": round(chip_temp_std, 4),
                        "bad_hash_count": bad_hash_count,
                        "double_hash_count": double_hash_count,
                        "read_errors": read_errors,
                        "event_codes": event_codes,
                        "expected_hashrate_ths": round(expected_hashrate, 4),
                    }
                )

                if step == 0:
                    latest_snapshot = {
                        "temperature": float(asic_temperature_c),
                        "instability": float(power_instability_index),
                        "hashrate_dev_pct": float(hashrate_deviation_pct),
                        "voltage": float(asic_voltage_v),
                    }

            temp_signal = _clamp((latest_snapshot["temperature"] - 70.0) / 40.0, 0.0, 1.35)
            instability_signal = _clamp(latest_snapshot["instability"], 0.0, 1.0)
            hashrate_drop_signal = _clamp((-latest_snapshot["hashrate_dev_pct"]) / 45.0, 0.0, 1.25)
            voltage_signal = _clamp((latest_snapshot["voltage"] - 12.5) / 1.8, 0.0, 1.0)
            risk_score = _clamp(
                0.12
                + 0.35 * temp_signal
                + 0.28 * instability_signal
                + 0.22 * hashrate_drop_signal
                + 0.10 * voltage_signal,
                0.03,
                0.99,
            )
            risk_band = _risk_band(risk_score)
            risk_rows.append(
                {
                    "predicted_at": now,
                    "miner_id": miner_id,
                    "risk_score": round(risk_score, 6),
                    "risk_band": risk_band,
                    "predicted_failure": bool(risk_score >= 0.55),
                    "model_version": DEMO_SEED_MODEL_VERSION,
                }
            )

    telemetry_df = pd.DataFrame(telemetry_rows)
    kpi_df = pd.DataFrame(kpi_rows)
    risk_df = pd.DataFrame(risk_rows)
    logger.info(
        "Generated demo seed payload rows=%s miners=%s",
        len(telemetry_df),
        kpi_df["miner_id"].nunique(),
    )
    return telemetry_df, kpi_df, risk_df


async def _read_upload_with_limit(upload: UploadFile) -> bytes:
    total = 0
    chunks: list[bytes] = []
    while True:
        chunk = await upload.read(INGEST_READ_CHUNK_BYTES)
        if not chunk:
            break
        total += len(chunk)
        if total > MAX_INGEST_FILE_BYTES:
            raise HTTPException(
                413,
                (
                    f"CSV payload too large ({total} bytes). "
                    f"Limit is {MAX_INGEST_FILE_BYTES} bytes."
                ),
            )
        chunks.append(chunk)
    return b"".join(chunks)


@router.post("/ingest/csv", response_model=IngestResult)
async def ingest_csv(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    content = await _read_upload_with_limit(file)
    try:
        df = pd.read_csv(io.StringIO(content.decode("utf-8")))
    except Exception as exc:
        raise HTTPException(400, f"Could not parse CSV: {exc}")

    if len(df) > MAX_INGEST_ROWS:
        raise HTTPException(
            413,
            (
                f"CSV has too many rows ({len(df)}). "
                f"Limit is {MAX_INGEST_ROWS} rows."
            ),
        )

    rows_received = len(df)
    df, errors = _validate_and_clean(df)

    if df.empty:
        return IngestResult(rows_received=rows_received, rows_inserted=0,
                            miners_found=[], errors=errors + ["No valid rows to insert"])

    payload = _build_ingest_payload(df)
    inserted_result = await db.execute(INGEST_JSON_INSERT_SQL, {"payload": payload})
    rows_inserted = int(inserted_result.scalar_one() or 0)
    await db.commit()

    miners = sorted(df["miner_id"].dropna().unique().tolist())
    return IngestResult(
        rows_received=rows_received,
        rows_inserted=rows_inserted,
        miners_found=miners,
        errors=errors,
    )


@router.post("/ingest/seed-demo", response_model=DemoSeedResult)
async def seed_demo_data(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    telemetry_df, kpi_df, risk_df = _generate_demo_seed_frames(
        miners=DEMO_SEED_MINERS,
        history_points=DEMO_SEED_HISTORY_POINTS,
    )

    try:
        telemetry_payload = _build_ingest_payload(telemetry_df)
        telemetry_result = await db.execute(
            INGEST_JSON_INSERT_SQL,
            {"payload": telemetry_payload},
        )
        rows_inserted = int(telemetry_result.scalar_one() or 0)

        kpi_payload = _build_kpi_payload(kpi_df)
        kpi_result = await db.execute(
            KPI_JSON_INSERT_SQL,
            {"payload": kpi_payload},
        )
        kpi_rows_inserted = int(kpi_result.scalar_one() or 0)

        risk_payload = _build_risk_payload(risk_df)
        risk_result = await db.execute(
            RISK_JSON_UPSERT_SQL,
            {"payload": risk_payload},
        )
        risk_rows_inserted = int(risk_result.scalar_one() or 0)
        await db.commit()
    except Exception as exc:
        await db.rollback()
        logger.exception("Demo seed ingestion failed")
        raise HTTPException(500, f"Demo seed ingestion failed: {exc}")

    miners = sorted(telemetry_df["miner_id"].dropna().unique().tolist())
    return DemoSeedResult(
        rows_received=len(telemetry_df),
        rows_inserted=rows_inserted,
        kpi_rows_inserted=kpi_rows_inserted,
        risk_rows_inserted=risk_rows_inserted,
        miners_found=miners,
        errors=[],
    )


# ── API Sources ────────────────────────────────────────────────────────────
@router.get("/ingest/sources/allowlist")
async def get_ingest_allowlist(
    _: str = Depends(verify_token),
):
    allowlist = get_source_allowlist()
    return {
        "allowlist": allowlist,
        "allowlist_configured": bool(allowlist),
    }


@router.post("/ingest/sources/validate-url")
async def validate_ingest_source_url(
    body: SourceUrlValidationIn,
    _: str = Depends(verify_token),
):
    return inspect_source_url(body.url_template)


@router.get("/ingest/sources")
async def list_sources(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    result = await db.execute(text("""
        SELECT id, name, url_template, auth_headers, field_mapping,
               polling_interval_minutes, enabled, last_fetched_at, created_at
        FROM api_sources ORDER BY id
    """))
    return [dict(r) for r in result.mappings().all()]


@router.post("/ingest/sources", response_model=ApiSourceOut)
async def create_source(
    body: ApiSourceIn,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    try:
        validate_source_url(body.url_template)
    except UnsafeURLError as exc:
        raise HTTPException(400, str(exc))

    result = await db.execute(
        text("""
            INSERT INTO api_sources
                (name, url_template, auth_headers, field_mapping,
                 polling_interval_minutes, enabled)
            VALUES (
                :name,
                :url,
                CAST(:auth AS jsonb),
                CAST(:mapping AS jsonb),
                :interval,
                :enabled
            )
            RETURNING id, name, url_template, auth_headers, field_mapping,
                      polling_interval_minutes, enabled, last_fetched_at, created_at
        """),
        {
            "name": body.name, "url": body.url_template,
            "auth": json.dumps(body.auth_headers),
            "mapping": json.dumps(body.field_mapping),
            "interval": body.polling_interval_minutes, "enabled": body.enabled,
        },
    )
    await db.commit()
    return dict(result.mappings().first())


@router.delete("/ingest/sources/{source_id}")
async def delete_source(
    source_id: int,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    result = await db.execute(
        text("DELETE FROM api_sources WHERE id = :id RETURNING id"), {"id": source_id}
    )
    row = result.mappings().first()
    if row is None:
        await db.rollback()
        raise HTTPException(status_code=404, detail="Source not found")

    await db.commit()
    return {"status": "deleted", "id": row["id"]}


@router.post("/ingest/sources/{source_id}/toggle")
async def toggle_source(
    source_id: int,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    result = await db.execute(
        text("""
            UPDATE api_sources SET enabled = NOT enabled
            WHERE id = :id RETURNING id, enabled
        """),
        {"id": source_id},
    )
    row = result.mappings().first()
    if row is None:
        await db.rollback()
        raise HTTPException(status_code=404, detail="Source not found")

    await db.commit()
    return dict(row)
