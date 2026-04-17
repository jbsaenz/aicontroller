"""CSV upload and external API source management endpoints."""

import io
import json
import logging
import os

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


class SourceUrlValidationIn(BaseModel):
    url_template: str


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


def _build_ingest_payload(df: pd.DataFrame) -> str:
    records = []
    for row in df.reindex(columns=TELEMETRY_INSERT_COLS).to_dict(orient="records"):
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
