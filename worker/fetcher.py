"""External API source poller."""

import logging
import math
import os
import threading
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse

import httpx
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.url_safety import UnsafeURLError, inspect_source_url, validate_source_url

logger = logging.getLogger("worker.fetcher")
_MISSING = object()
SOURCE_FETCH_FAILURE_DISABLE_THRESHOLD = max(
    int(os.getenv("SOURCE_FETCH_FAILURE_DISABLE_THRESHOLD", "5")), 1
)
SOURCE_FETCH_BACKOFF_MAX_MULTIPLIER = max(
    int(os.getenv("SOURCE_FETCH_BACKOFF_MAX_MULTIPLIER", "16")), 1
)
_FETCHER_SCHEMA_READY = False
_FETCHER_SCHEMA_LOCK = threading.Lock()


def _pinned_http_url(source_url: str, pinned_ip: str) -> tuple[str, str]:
    parsed = urlparse(source_url)
    if not parsed.hostname:
        raise UnsafeURLError("Source URL must include hostname")
    host_header = parsed.hostname
    if parsed.port and parsed.port not in {80, 443}:
        host_header = f"{parsed.hostname}:{parsed.port}"
    netloc = pinned_ip if not parsed.port else f"{pinned_ip}:{parsed.port}"
    pinned = urlunparse(
        (parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)
    )
    return pinned, host_header


def _failure_backoff_minutes(interval_minutes: float, streak: int) -> float:
    if streak <= 0:
        return max(interval_minutes, 0.0)
    multiplier = min(2 ** streak, SOURCE_FETCH_BACKOFF_MAX_MULTIPLIER)
    return max(interval_minutes, 1.0) * float(multiplier)


def _ensure_fetch_backoff_columns(engine: Engine) -> None:
    """No-op: columns are now declared in docker/db/init.sql.

    Retained as a call-site stub to avoid breaking callers during the
    transition. Safe to remove once all deployments have run init.sql.
    """
    global _FETCHER_SCHEMA_READY
    _FETCHER_SCHEMA_READY = True


def _clear_failure_tracking(engine: Engine, source_id: int) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE api_sources
                SET fetch_failure_streak = 0,
                    last_fetch_attempt_at = NULL
                WHERE id = :id
            """),
            {"id": source_id},
        )


def _record_fetch_failure(engine: Engine, source: dict, reason: str) -> None:
    source_id = int(source["id"])
    source_name = source.get("name", f"id={source_id}")
    streak = int(source.get("fetch_failure_streak") or 0) + 1

    if streak < SOURCE_FETCH_FAILURE_DISABLE_THRESHOLD:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE api_sources
                    SET fetch_failure_streak = :streak,
                        last_fetch_attempt_at = NOW()
                    WHERE id = :id
                """),
                {"id": source_id, "streak": streak},
            )
        logger.warning(
            "Fetch failed for source '%s' (streak=%d/%d): %s",
            source_name,
            streak,
            SOURCE_FETCH_FAILURE_DISABLE_THRESHOLD,
            reason,
        )
        return

    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE api_sources
                SET enabled = FALSE,
                    fetch_failure_streak = 0,
                    last_fetch_attempt_at = NOW()
                WHERE id = :id
            """),
            {"id": source_id},
        )
    logger.error(
        "Source '%s' auto-disabled after %d consecutive fetch failures: %s",
        source_name,
        SOURCE_FETCH_FAILURE_DISABLE_THRESHOLD,
        reason,
    )


def run_fetch_job(engine: Engine):
    _ensure_fetch_backoff_columns(engine)
    with engine.connect() as conn:
        try:
            result = conn.execute(text("""
                SELECT id, name, url_template, auth_headers, field_mapping,
                       polling_interval_minutes, last_fetched_at,
                       fetch_failure_streak, last_fetch_attempt_at
                FROM api_sources
                WHERE enabled = TRUE
            """))
            sources = result.mappings().all()
        except Exception:
            result = conn.execute(text("""
                SELECT id, name, url_template, auth_headers, field_mapping,
                       polling_interval_minutes, last_fetched_at
                FROM api_sources
                WHERE enabled = TRUE
            """))
            sources = []
            for row in result.mappings().all():
                normalized = dict(row)
                normalized["fetch_failure_streak"] = 0
                normalized["last_fetch_attempt_at"] = None
                sources.append(normalized)

    if not sources:
        return

    now = datetime.now(tz=timezone.utc)
    for source in sources:
        source_id = int(source["id"])
        last = source["last_fetched_at"]
        interval = float(source["polling_interval_minutes"] or 10)
        streak = int(source.get("fetch_failure_streak") or 0)
        effective_interval = (
            _failure_backoff_minutes(interval, streak)
            if streak > 0
            else max(interval, 0.0)
        )

        last_attempt = source.get("last_fetch_attempt_at")
        reference_ts = _ensure_utc_timestamp(last_attempt or last) if (last_attempt or last) else None
        if reference_ts is not None:
            elapsed = (now - reference_ts).total_seconds() / 60
            if elapsed < effective_interval:
                continue

        try:
            _fetch_and_store(engine, dict(source))
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE api_sources
                        SET last_fetched_at = NOW(),
                            last_fetch_attempt_at = NOW(),
                            fetch_failure_streak = 0
                        WHERE id = :id
                    """),
                    {"id": source_id},
                )
        except UnsafeURLError as exc:
            _record_fetch_failure(engine, dict(source), str(exc))
        except Exception as exc:
            _record_fetch_failure(engine, dict(source), str(exc))


def _fetch_and_store(engine: Engine, source: dict):
    headers = source.get("auth_headers") or {}
    mapping = source.get("field_mapping") or {}
    source_url = source["url_template"]

    # Re-validate at fetch time to prevent stale/unsafe configs and DNS changes.
    validation = inspect_source_url(source_url)
    if not validation.get("valid"):
        raise UnsafeURLError("; ".join(validation.get("errors", [])))
    resolved_ips = validation.get("resolved_ips") or []
    if not resolved_ips:
        raise UnsafeURLError("Source hostname resolved to no usable addresses")

    request_url = source_url
    request_headers = dict(headers)
    parsed = urlparse(source_url)
    if parsed.scheme == "http":
        # Best-effort DNS pinning for HTTP sources to reduce rebinding window.
        request_url, host_header = _pinned_http_url(source_url, resolved_ips[0])
        request_headers["Host"] = host_header
    else:
        # For HTTPS, cert validation requires hostname URL. Re-check DNS immediately
        # before request and block on drift.
        recheck = inspect_source_url(source_url)
        if not recheck.get("valid"):
            raise UnsafeURLError("; ".join(recheck.get("errors", [])))
        recheck_ips = set(recheck.get("resolved_ips") or [])
        if set(resolved_ips) != recheck_ips:
            raise UnsafeURLError(
                "Source hostname DNS changed during preflight; blocking request"
            )

    with httpx.Client(timeout=30, follow_redirects=False, trust_env=False) as client:
        resp = client.get(request_url, headers=request_headers)
        resp.raise_for_status()
        data = resp.json()

    # Data can be a list of miner records or a single dict
    if isinstance(data, dict):
        data = [data]

    records = []
    skipped = 0
    for item in data:
        miner_id = _normalize_text(_extract(item, mapping.get("miner_id", "miner_id"), None))
        timestamp = _parse_timestamp(
            _extract(item, mapping.get("timestamp", "timestamp"), None)
        )
        if not miner_id or timestamp is None:
            skipped += 1
            continue

        row = {
            "miner_id": miner_id,
            "timestamp": timestamp,
            "asic_clock_mhz": _to_float(item, mapping.get("asic_clock_mhz", "asic_clock_mhz")),
            "asic_voltage_v": _to_float(item, mapping.get("asic_voltage_v", "asic_voltage_v")),
            "asic_hashrate_ths": _to_float(item, mapping.get("asic_hashrate_ths", "asic_hashrate_ths")),
            "asic_temperature_c": _to_float(item, mapping.get("asic_temperature_c", "asic_temperature_c")),
            "asic_power_w": _to_float(item, mapping.get("asic_power_w", "asic_power_w")),
            "operating_mode": _normalize_text(
                _extract(item, mapping.get("operating_mode", "operating_mode"), "normal")
            ) or "normal",
            "chip_temp_max": _to_float(item, mapping.get("chip_temp_max", "chip_temp_max")),
            "chip_temp_std": _to_float(item, mapping.get("chip_temp_std", "chip_temp_std")),
            "bad_hash_count": _to_int(item, mapping.get("bad_hash_count", "bad_hash_count")),
            "double_hash_count": _to_int(item, mapping.get("double_hash_count", "double_hash_count")),
            "read_errors": _to_int(item, mapping.get("read_errors", "read_errors")),
            "event_codes": _normalize_text(
                _extract(item, mapping.get("event_codes", "event_codes"), None)
            ),
            "expected_hashrate_ths": _to_float(item, mapping.get("expected_hashrate_ths", "expected_hashrate_ths")),
            "source": f"api:{source['name']}",
        }
        records.append(row)

    if not records:
        if skipped:
            logger.warning(
                "Source '%s': all %d rows were invalid (missing miner_id/timestamp)",
                source["name"],
                skipped,
            )
        return

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO telemetry
                    (miner_id, timestamp, asic_clock_mhz, asic_voltage_v,
                     asic_hashrate_ths, asic_temperature_c, asic_power_w,
                     operating_mode, chip_temp_max, chip_temp_std,
                     bad_hash_count, double_hash_count, read_errors,
                     event_codes, expected_hashrate_ths, source)
                VALUES
                    (:miner_id, :timestamp, :asic_clock_mhz, :asic_voltage_v,
                     :asic_hashrate_ths, :asic_temperature_c, :asic_power_w,
                     :operating_mode, :chip_temp_max, :chip_temp_std,
                     :bad_hash_count, :double_hash_count, :read_errors,
                     :event_codes, :expected_hashrate_ths, :source)
                ON CONFLICT (miner_id, timestamp) DO NOTHING
            """),
            records,
        )
    if skipped:
        logger.warning(
            "Source '%s': fetched %d rows, skipped %d invalid rows",
            source["name"],
            len(records),
            skipped,
        )
    else:
        logger.info("Fetched %d rows from source '%s'", len(records), source["name"])


def _extract(item: dict, key: str, default):
    return item.get(key, default)


def _normalize_text(value):
    if value is None:
        return None
    text_value = str(value).strip()
    if not text_value or text_value.lower() in {"nan", "none", "null"}:
        return None
    return text_value


def _parse_timestamp(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_utc_timestamp(value)

    text_value = str(value).strip()
    if not text_value:
        return None
    try:
        parsed = datetime.fromisoformat(text_value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc_timestamp(parsed)


def _ensure_utc_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _to_float(item: dict, key: str):
    raw = item.get(key, _MISSING)
    if raw is _MISSING or raw is None:
        return None
    if isinstance(raw, str):
        raw = raw.strip()
        if raw == "":
            return None
    try:
        numeric = float(raw)
    except (ValueError, TypeError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _to_int(item: dict, key: str):
    raw = item.get(key, _MISSING)
    if raw is _MISSING or raw is None:
        return None
    if isinstance(raw, str):
        raw = raw.strip()
        if raw == "":
            return None
    try:
        numeric = float(raw)
    except (ValueError, TypeError):
        return None
    if not math.isfinite(numeric):
        return None
    return int(numeric)
