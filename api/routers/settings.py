"""Application settings CRUD endpoints."""

import json
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import verify_token
from api.db import get_db
from api.schemas import SettingsOut, SettingsUpdate
from src.secret_store import encrypt_if_needed

router = APIRouter()

BOOLEAN_KEYS = {
    "policy_optimizer_enabled",
    "automation_require_policy_backtest",
}
INTEGER_RANGES = {
    "smtp_port": (1, 65535),
    "alert_cooldown_hours": (0, 168),
    "retrain_days": (1, 3650),
}
FLOAT_RANGES = {
    "risk_threshold": (0.05, 0.99),
    "policy_min_uplift_usd_per_miner": (0.0, 1000.0),
    "hashprice_usd_per_ph_day": (0.0, 100000.0),
    "opex_usd_per_mwh": (0.0, 10000.0),
    "capex_usd_per_mwh": (0.0, 10000.0),
    "energy_price_usd_per_kwh": (0.0, 5.0),
    "curtailment_penalty_multiplier": (1.0, 20.0),
    "policy_reward_per_th_hour_usd": (0.0, 1000.0),
    "policy_failure_cost_usd": (0.0, 1000000.0),
    "policy_horizon_hours": (0.25, 168.0),
    "risk_probability_horizon_hours": (1.0, 720.0),
}
JSON_OBJECT_KEYS = {"energy_price_schedule_json"}
JSON_ARRAY_KEYS = {"curtailment_windows_json"}
TIMEZONE_KEYS = {"policy_timezone"}
TEXT_KEYS = {
    "smtp_host",
    "smtp_user",
    "smtp_password",
    "alert_from_email",
    "alert_to_emails",
    "telegram_bot_token",
    "telegram_chat_id",
}
SENSITIVE_TEXT_KEYS = {"smtp_password", "telegram_bot_token"}
SECRET_MASK = "********"
ALLOWED_SETTING_KEYS = (
    BOOLEAN_KEYS
    | set(INTEGER_RANGES)
    | set(FLOAT_RANGES)
    | JSON_OBJECT_KEYS
    | JSON_ARRAY_KEYS
    | TIMEZONE_KEYS
    | TEXT_KEYS
)
MAX_TEXT_LENGTH = 4096


def _mask_secret_value(value: str | None) -> str:
    text_value = "" if value is None else str(value)
    return SECRET_MASK if text_value != "" else "(not configured)"


def _validate_setting_value(key: str, value) -> str:
    text_value = "" if value is None else str(value).strip()

    if key in BOOLEAN_KEYS:
        if text_value == "":
            return ""
        normalized = text_value.lower()
        if normalized not in {"1", "0", "true", "false", "yes", "no", "on", "off"}:
            raise HTTPException(400, f"Invalid boolean for '{key}'")
        return normalized

    if key in INTEGER_RANGES:
        if text_value == "":
            return ""
        try:
            parsed = int(float(text_value))
        except (TypeError, ValueError):
            raise HTTPException(400, f"Invalid integer for '{key}'")
        min_val, max_val = INTEGER_RANGES[key]
        if parsed < min_val or parsed > max_val:
            raise HTTPException(400, f"Value for '{key}' must be between {min_val} and {max_val}")
        return str(parsed)

    if key in FLOAT_RANGES:
        if text_value == "":
            return ""
        try:
            parsed = float(text_value)
        except (TypeError, ValueError):
            raise HTTPException(400, f"Invalid number for '{key}'")
        min_val, max_val = FLOAT_RANGES[key]
        if parsed < min_val or parsed > max_val:
            raise HTTPException(400, f"Value for '{key}' must be between {min_val} and {max_val}")
        return str(parsed)

    if key in JSON_OBJECT_KEYS:
        if text_value == "":
            return "{}"
        try:
            parsed = json.loads(text_value)
        except Exception:
            raise HTTPException(400, f"Invalid JSON object for '{key}'")
        if not isinstance(parsed, dict):
            raise HTTPException(400, f"Setting '{key}' must be a JSON object")
        return json.dumps(parsed, separators=(",", ":"))

    if key in JSON_ARRAY_KEYS:
        if text_value == "":
            return "[]"
        try:
            parsed = json.loads(text_value)
        except Exception:
            raise HTTPException(400, f"Invalid JSON array for '{key}'")
        if not isinstance(parsed, list):
            raise HTTPException(400, f"Setting '{key}' must be a JSON array")
        return json.dumps(parsed, separators=(",", ":"))

    if key in TIMEZONE_KEYS:
        if text_value == "":
            return "UTC"
        try:
            ZoneInfo(text_value)
        except Exception:
            raise HTTPException(400, f"Invalid timezone for '{key}'")
        return text_value

    if key in TEXT_KEYS:
        if len(text_value) > MAX_TEXT_LENGTH:
            raise HTTPException(400, f"Value for '{key}' is too long")
        return text_value

    raise HTTPException(400, f"Unsupported setting key '{key}'")


@router.get("/settings", response_model=SettingsOut)
async def get_settings(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    result = await db.execute(text("SELECT key, value FROM app_settings ORDER BY key"))
    rows = result.mappings().all()
    settings = {}
    for row in rows:
        key = row["key"]
        value = row["value"]
        if key in SENSITIVE_TEXT_KEYS:
            value = _mask_secret_value(value)
        settings[key] = value
    return SettingsOut(settings=settings)


@router.put("/settings")
async def update_settings(
    body: SettingsUpdate,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    if len(body.settings) > len(ALLOWED_SETTING_KEYS):
        raise HTTPException(400, "Too many settings in one request")

    unknown_keys = sorted(set(body.settings) - ALLOWED_SETTING_KEYS)
    if unknown_keys:
        raise HTTPException(400, f"Unknown setting keys: {unknown_keys}")

    cleaned_settings = {
        key: _validate_setting_value(key, value)
        for key, value in body.settings.items()
    }

    updated_keys: list[str] = []
    for key, value in cleaned_settings.items():
        if key in SENSITIVE_TEXT_KEYS:
            if value == SECRET_MASK:
                continue
            try:
                value_to_store = encrypt_if_needed(value) if value != "" else ""
            except RuntimeError as exc:
                raise HTTPException(500, str(exc))
        else:
            value_to_store = value
        await db.execute(
            text("""
                INSERT INTO app_settings (key, value, updated_at)
                VALUES (:k, :v, NOW())
                ON CONFLICT (key) DO UPDATE SET value = :v, updated_at = NOW()
            """),
            {"k": key, "v": value_to_store},
        )
        updated_keys.append(key)
    await db.commit()
    return {"status": "ok", "updated": updated_keys}


@router.get("/settings/{key}")
async def get_setting(
    key: str,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    if key not in ALLOWED_SETTING_KEYS:
        raise HTTPException(status_code=404, detail="Setting key not found")

    result = await db.execute(
        text("SELECT value FROM app_settings WHERE key = :k"),
        {"k": key},
    )
    row = result.mappings().first()
    if not row:
        return {"key": key, "value": None}
    value = row["value"]
    if key in SENSITIVE_TEXT_KEYS:
        value = _mask_secret_value(value)
    return {"key": key, "value": value}
