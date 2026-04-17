"""Automated action execution engine."""

import logging
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import httpx
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.runtime_utils import is_truthy, normalize_control_mode
from src.url_safety import UnsafeURLError, validate_automator_url

logger = logging.getLogger("worker.automator")
AUTOMATOR_FAILURE_THRESHOLD = max(
    int(os.getenv("AUTOMATOR_FAILURE_THRESHOLD", "3")),
    1,
)
AUTOMATOR_BACKOFF_BASE_SECONDS = max(
    float(os.getenv("AUTOMATOR_BACKOFF_BASE_SECONDS", "30")),
    1.0,
)
AUTOMATOR_BACKOFF_MAX_SECONDS = max(
    float(os.getenv("AUTOMATOR_BACKOFF_MAX_SECONDS", "900")),
    AUTOMATOR_BACKOFF_BASE_SECONDS,
)
AUTOMATOR_CIRCUIT_OPEN_SECONDS = max(
    float(os.getenv("AUTOMATOR_CIRCUIT_OPEN_SECONDS", "300")),
    AUTOMATOR_BACKOFF_BASE_SECONDS,
)
AUTOMATOR_FAILURE_STREAK_KEY = "automator_failure_streak"
AUTOMATOR_CIRCUIT_OPEN_UNTIL_KEY = "automator_circuit_open_until"
AUTOMATOR_LAST_FAILURE_REASON_KEY = "automator_last_failure_reason"
AUTOMATOR_STATE_KEYS = (
    AUTOMATOR_FAILURE_STREAK_KEY,
    AUTOMATOR_CIRCUIT_OPEN_UNTIL_KEY,
    AUTOMATOR_LAST_FAILURE_REASON_KEY,
)


class _RemoteEndpointError(Exception):
    """Raised when remote automator endpoint calls fail."""


def _compute_backoff_seconds(streak: int) -> float:
    exponent = max(streak - 1, 0)
    return min(
        AUTOMATOR_BACKOFF_BASE_SECONDS * float(2 ** exponent),
        AUTOMATOR_BACKOFF_MAX_SECONDS,
    )


def _parse_utc_timestamp(value) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text_value = str(value).strip()
        if text_value.endswith("Z"):
            text_value = text_value[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text_value)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _extract_state_key_value(row) -> tuple[str | None, str | None]:
    if isinstance(row, dict):
        return row.get("key"), row.get("value")
    if hasattr(row, "_mapping"):
        mapping = row._mapping
        return mapping.get("key"), mapping.get("value")
    return None, None


def _read_automator_state(engine: Engine) -> tuple[int, datetime | None, str]:
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT key, value
                    FROM app_settings
                    WHERE key = ANY(CAST(:keys AS text[]))
                """),
                {"keys": list(AUTOMATOR_STATE_KEYS)},
            )
            if hasattr(result, "mappings"):
                raw_rows = result.mappings().all()
            else:
                raw_rows = list(result)
    except Exception as exc:
        logger.warning("Automator could not read persistent retry state: %s", exc)
        return 0, None, ""

    state: dict[str, str] = {}
    for row in raw_rows:
        key, value = _extract_state_key_value(row)
        if key:
            state[str(key)] = "" if value is None else str(value)

    try:
        streak = max(int(float(state.get(AUTOMATOR_FAILURE_STREAK_KEY, "0"))), 0)
    except (TypeError, ValueError):
        streak = 0
    open_until = _parse_utc_timestamp(state.get(AUTOMATOR_CIRCUIT_OPEN_UNTIL_KEY, ""))
    reason = state.get(AUTOMATOR_LAST_FAILURE_REASON_KEY, "")
    return streak, open_until, reason


def _write_automator_state(
    engine: Engine,
    *,
    streak: int,
    circuit_open_until: datetime | None,
    reason: str,
) -> None:
    rows = [
        {
            "k": AUTOMATOR_FAILURE_STREAK_KEY,
            "v": str(max(int(streak), 0)),
        },
        {
            "k": AUTOMATOR_CIRCUIT_OPEN_UNTIL_KEY,
            "v": circuit_open_until.isoformat() if circuit_open_until else "",
        },
        {
            "k": AUTOMATOR_LAST_FAILURE_REASON_KEY,
            "v": str(reason or ""),
        },
    ]
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO app_settings (key, value, updated_at)
                    VALUES (:k, :v, NOW())
                    ON CONFLICT (key) DO UPDATE
                    SET value = EXCLUDED.value, updated_at = NOW()
                """),
                rows,
            )
    except Exception as exc:
        logger.warning("Automator could not persist retry state: %s", exc)


def _record_remote_failure(engine: Engine, reason: str) -> None:
    now = datetime.now(tz=timezone.utc)
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO app_settings (key, value, updated_at)
                    VALUES (:k, '1', NOW())
                    ON CONFLICT (key) DO UPDATE
                    SET value = (COALESCE(app_settings.value::int, 0) + 1)::text,
                        updated_at = NOW()
                    RETURNING value::int AS new_streak
                """),
                {"k": AUTOMATOR_FAILURE_STREAK_KEY},
            )
            streak = int(result.scalar_one())
    except Exception as exc:
        logger.warning("Automator could not atomically increment failure streak: %s", exc)
        prior_streak, _, _ = _read_automator_state(engine)
        streak = prior_streak + 1

    pause_seconds = _compute_backoff_seconds(streak)
    if streak >= AUTOMATOR_FAILURE_THRESHOLD:
        pause_seconds = max(pause_seconds, AUTOMATOR_CIRCUIT_OPEN_SECONDS)
    circuit_open_until = now + timedelta(seconds=pause_seconds)
    _write_automator_state(
        engine,
        streak=streak,
        circuit_open_until=circuit_open_until,
        reason=reason,
    )

    if streak >= AUTOMATOR_FAILURE_THRESHOLD:
        logger.error(
            "Automator circuit opened for %.1fs after %d consecutive remote failures: %s",
            pause_seconds,
            streak,
            reason,
        )
    else:
        logger.warning(
            "Automator backoff %.1fs (streak=%d/%d) due to remote failure: %s",
            pause_seconds,
            streak,
            AUTOMATOR_FAILURE_THRESHOLD,
            reason,
        )


def _record_remote_success(engine: Engine) -> None:
    _write_automator_state(
        engine,
        streak=0,
        circuit_open_until=None,
        reason="",
    )


def _remote_circuit_status(engine: Engine) -> tuple[bool, float, str]:
    now = datetime.now(tz=timezone.utc)
    streak, circuit_open_until, reason = _read_automator_state(engine)
    if circuit_open_until is None:
        return False, 0.0, reason
    remaining = (circuit_open_until - now).total_seconds()
    if remaining <= 0:
        _write_automator_state(
            engine,
            streak=streak,
            circuit_open_until=None,
            reason=reason,
        )
        return False, 0.0, reason
    return True, remaining, reason


def _load_control_mode(engine: Engine) -> str:
    mode = normalize_control_mode(os.getenv("CONTROL_MODE", "advisory"))
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT value FROM app_settings WHERE key = 'control_mode'")
            )
            row = result.mappings().first()
        if row and row.get("value") not in (None, ""):
            mode = normalize_control_mode(row["value"])
    except Exception as exc:
        logger.warning("Could not read control_mode from DB; using env: %s", exc)

    return normalize_control_mode(mode)


def run_automator_job(engine: Engine):
    """Scan alerts for automated actions and simulate their execution."""
    control_mode = _load_control_mode(engine)
    if control_mode != "actuation":
        logger.info("Automator disabled in CONTROL_MODE=%s", control_mode)
        return

    simulation_mode = is_truthy(os.getenv("AUTOMATOR_SIMULATION", "true"))
    if not simulation_mode:
        blocked, remaining_seconds, reason = _remote_circuit_status(engine)
        if blocked:
            logger.warning(
                "Automator skipped: circuit open for %.1fs after remote failure (%s)",
                remaining_seconds,
                reason or "unknown",
            )
            return

    with engine.connect() as conn:
        # Fetch pending automated actions
        result = conn.execute(text("""
            SELECT id, miner_id, recommended_action
            FROM alerts
            WHERE resolved = FALSE
              AND automation_triggered = TRUE
            ORDER BY created_at ASC
            LIMIT 50
        """))
        pending = result.mappings().all()

    if not pending:
        return

    logger.info("Automator: Found %d pending automated actions", len(pending))

    for idx, alert in enumerate(pending):
        if not simulation_mode:
            blocked, remaining_seconds, reason = _remote_circuit_status(engine)
            if blocked:
                skipped = len(pending) - idx
                logger.warning(
                    "Automator pausing run: circuit open for %.1fs (%d pending action(s) deferred, reason=%s)",
                    remaining_seconds,
                    skipped,
                    reason or "unknown",
                )
                break

        alert_id = alert["id"]
        miner_id = alert["miner_id"]
        action = alert["recommended_action"]

        success = False
        acknowledged = False
        try:
            success, acknowledged = _execute_action(
                miner_id=miner_id,
                action=action,
                simulation_mode=simulation_mode,
            )
            if success and not simulation_mode:
                _record_remote_success(engine)
            elif not success and not simulation_mode:
                _record_remote_failure(engine, "remote action did not execute")
        except _RemoteEndpointError as e:
            logger.error("Automator: Remote call failed for %s on %s: %s", action, miner_id, e)
            if not simulation_mode:
                _record_remote_failure(engine, str(e))
        except Exception as e:
            logger.error("Automator: Unexpected local error for %s on %s: %s", action, miner_id, e)

        if acknowledged:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE alerts
                        SET resolved = TRUE, resolved_at = NOW()
                        WHERE id = :id
                    """),
                    {"id": alert_id},
                )
            logger.info("Automator: Successfully executed %s on %s and resolved alert %d", action, miner_id, alert_id)
        elif success:
            logger.info(
                "Automator: Executed %s on %s but no external ack; leaving alert %d active",
                action,
                miner_id,
                alert_id,
            )
        else:
            logger.warning("Automator: Action %s on %s failed, keeping alert %d active", action, miner_id, alert_id)


def _format_endpoint(template: str, miner_id: str, action: str) -> str:
    safe_miner_id = quote(str(miner_id), safe="")
    safe_action = quote(str(action), safe="")
    return template.format(miner_id=safe_miner_id, action=safe_action)


def _execute_action(miner_id: str, action: str, simulation_mode: bool) -> tuple[bool, bool]:
    """Execute action and return (executed, externally_acknowledged)."""
    logger.info("Executing action: %s on miner: %s", action, miner_id)

    if simulation_mode:
        logger.info("Automator simulation mode enabled; resolving alert after simulated ack")
        return True, True

    endpoint_template = os.getenv("AUTOMATOR_ENDPOINT_TEMPLATE", "").strip()
    if not endpoint_template:
        logger.warning(
            "Automator production mode enabled but AUTOMATOR_ENDPOINT_TEMPLATE is unset"
        )
        return False, False

    try:
        endpoint = _format_endpoint(endpoint_template, miner_id, action)
    except Exception as exc:
        logger.warning("Automator endpoint template formatting failed: %s", exc)
        return False, False

    try:
        validate_automator_url(endpoint)
    except UnsafeURLError as exc:
        logger.warning("Automator endpoint blocked by URL safety policy: %s", exc)
        return False, False

    payload = {"miner_id": miner_id, "action": action}
    with httpx.Client(timeout=10, follow_redirects=False, trust_env=False) as client:
        try:
            response = client.post(endpoint, json=payload)
            response.raise_for_status()
        except Exception as exc:
            raise _RemoteEndpointError(str(exc)) from exc

    try:
        body = response.json()
    except Exception:
        body = {}
    acknowledged = bool(body.get("acknowledged", False))
    return True, acknowledged
