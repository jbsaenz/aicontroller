"""External API source poller."""

import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.url_safety import UnsafeURLError, validate_source_url

logger = logging.getLogger("worker.fetcher")


def run_fetch_job(engine: Engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, name, url_template, auth_headers, field_mapping,
                   polling_interval_minutes, last_fetched_at
            FROM api_sources
            WHERE enabled = TRUE
        """))
        sources = result.mappings().all()

    if not sources:
        return

    now = datetime.now(tz=timezone.utc)
    for source in sources:
        last = source["last_fetched_at"]
        interval = source["polling_interval_minutes"] or 10

        if last:
            # Skip if fetched recently
            elapsed = (now - last.replace(tzinfo=timezone.utc)).total_seconds() / 60
            if elapsed < interval:
                continue

        try:
            _fetch_and_store(engine, dict(source))
            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE api_sources SET last_fetched_at = NOW() WHERE id = :id"),
                    {"id": source["id"]},
                )
        except UnsafeURLError as exc:
            logger.error("Fetch blocked for source %s: %s", source["name"], exc)
        except Exception as exc:
            logger.error("Fetch failed for source %s: %s", source["name"], exc)


def _fetch_and_store(engine: Engine, source: dict):
    headers = source.get("auth_headers") or {}
    mapping = source.get("field_mapping") or {}
    source_url = source["url_template"]

    # Re-validate at fetch time to prevent stale/unsafe configs and DNS changes.
    validate_source_url(source_url)

    with httpx.Client(timeout=30, follow_redirects=False, trust_env=False) as client:
        resp = client.get(source_url, headers=headers)
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
                ON CONFLICT DO NOTHING
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
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    text_value = str(value).strip()
    if not text_value:
        return None
    try:
        parsed = datetime.fromisoformat(text_value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _to_float(item: dict, key: str):
    try:
        return float(item.get(key, 0) or 0)
    except (ValueError, TypeError):
        return None


def _to_int(item: dict, key: str):
    try:
        return int(item.get(key, 0) or 0)
    except (ValueError, TypeError):
        return None
