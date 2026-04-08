"""Automated action execution engine."""

import logging
import os
import httpx
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger("worker.automator")


def _load_control_mode(engine: Engine) -> str:
    mode = os.getenv("CONTROL_MODE", "advisory").strip().lower()
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT value FROM app_settings WHERE key = 'control_mode'")
            )
            row = result.mappings().first()
        if row and row.get("value") not in (None, ""):
            mode = str(row["value"]).strip().lower()
    except Exception as exc:
        logger.warning("Could not read control_mode from DB; using env: %s", exc)

    if mode not in {"advisory", "actuation"}:
        return "advisory"
    return mode


def run_automator_job(engine: Engine):
    """Scan alerts for automated actions and simulate their execution."""
    control_mode = _load_control_mode(engine)
    if control_mode != "actuation":
        logger.info("Automator disabled in CONTROL_MODE=%s", control_mode)
        return

    simulation_mode = _is_truthy(os.getenv("AUTOMATOR_SIMULATION", "true"))

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

    for alert in pending:
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
        except Exception as e:
            logger.error("Automator: Failed to execute %s on %s: %s", action, miner_id, e)

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


def _is_truthy(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


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

    endpoint = endpoint_template.format(miner_id=miner_id, action=action)
    payload = {"miner_id": miner_id, "action": action}
    with httpx.Client(timeout=10, follow_redirects=False, trust_env=False) as client:
        response = client.post(endpoint, json=payload)
        response.raise_for_status()

    try:
        body = response.json()
    except Exception:
        body = {}
    acknowledged = bool(body.get("acknowledged", False))
    return True, acknowledged
