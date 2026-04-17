"""Policy optimization utilities for alert-action decisioning."""

from __future__ import annotations

import json
from collections.abc import Mapping
from collections import Counter
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from src.runtime_utils import is_truthy


ACTIONS = [
    "CONTINUE",
    "WATCHLIST",
    "DOWNCLOCK",
    "REBOOT",
    "PULL_FOR_MAINTENANCE",
]

ACTION_EFFECTS: dict[str, dict[str, float]] = {
    "CONTINUE": {
        "power_multiplier": 1.00,
        "hashrate_multiplier": 1.00,
        "risk_multiplier": 1.00,
        "intervention_cost_usd": 0.0,
        "downtime_hours": 0.0,
    },
    "WATCHLIST": {
        "power_multiplier": 1.00,
        "hashrate_multiplier": 1.00,
        "risk_multiplier": 0.96,
        "intervention_cost_usd": 0.2,
        "downtime_hours": 0.0,
    },
    "DOWNCLOCK": {
        "power_multiplier": 0.88,
        "hashrate_multiplier": 0.93,
        "risk_multiplier": 0.78,
        "intervention_cost_usd": 1.2,
        "downtime_hours": 0.0,
    },
    "REBOOT": {
        "power_multiplier": 0.95,
        "hashrate_multiplier": 0.97,
        "risk_multiplier": 0.62,
        "intervention_cost_usd": 2.8,
        "downtime_hours": 0.15,
    },
    "PULL_FOR_MAINTENANCE": {
        "power_multiplier": 0.0,
        "hashrate_multiplier": 0.0,
        "risk_multiplier": 0.55,
        "intervention_cost_usd": 35.0,
        "downtime_hours": 1.5,
    },
}

DEFAULT_HASHPRICE_USD_PER_PH_DAY = 55.0
DEFAULT_CURTAILMENT_PENALTY_MULTIPLIER = 2.0
DEFAULT_POLICY_FAILURE_COST_USD = 300.0
DEFAULT_POLICY_REWARD_PER_TH_HOUR_USD = DEFAULT_HASHPRICE_USD_PER_PH_DAY / 1000.0 / 24.0


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_str(value: Any, default: str) -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _reward_from_hashprice(hashprice_usd_per_ph_day: float) -> float:
    return max(0.0, hashprice_usd_per_ph_day / 1000.0 / 24.0)


def _parse_json(raw: Any, fallback: Any) -> Any:
    if raw in (None, ""):
        return fallback
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(str(raw))
    except Exception:
        return fallback


def _parse_hourly_price_schedule(raw: Any) -> dict[int, float]:
    schedule = {}
    parsed = _parse_json(raw, {})
    if isinstance(parsed, dict):
        for key, value in parsed.items():
            try:
                hour = int(key)
            except (TypeError, ValueError):
                continue
            if 0 <= hour <= 23:
                schedule[hour] = max(0.0, _to_float(value, 0.0))
    elif isinstance(parsed, list):
        for item in parsed:
            if not isinstance(item, dict):
                continue
            hour = int(_to_float(item.get("hour"), -1))
            if 0 <= hour <= 23:
                schedule[hour] = max(0.0, _to_float(item.get("price"), 0.0))
    return schedule


def _parse_curtailment_windows(raw: Any) -> list[tuple[int, int]]:
    windows: list[tuple[int, int]] = []
    parsed = _parse_json(raw, [])
    if not isinstance(parsed, list):
        return windows
    for item in parsed:
        if not isinstance(item, dict):
            continue
        start_hour = int(_to_float(item.get("start_hour"), -1))
        end_hour = int(_to_float(item.get("end_hour"), -1))
        if (
            0 <= start_hour <= 23
            and 0 <= end_hour <= 23
            and start_hour != end_hour
        ):
            windows.append((start_hour, end_hour))
    return windows


def parse_policy_config(raw: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Normalize policy runtime config from env/db values."""
    data = dict(raw or {})
    hashprice_usd_per_ph_day = max(
        0.0,
        _to_float(
            data.get("hashprice_usd_per_ph_day"),
            DEFAULT_HASHPRICE_USD_PER_PH_DAY,
        ),
    )
    reward_default = _reward_from_hashprice(hashprice_usd_per_ph_day)
    reward_raw = data.get("policy_reward_per_th_hour_usd")
    reward_per_th_hour = (
        reward_default
        if reward_raw is None or str(reward_raw).strip() == ""
        else max(0.0, _to_float(reward_raw, reward_default))
    )

    return {
        "optimizer_enabled": is_truthy(
            data.get("policy_optimizer_enabled"), default=True
        ),
        "require_backtest_for_automation": is_truthy(
            data.get("automation_require_policy_backtest"), default=True
        ),
        "min_uplift_usd_per_miner": _to_float(
            data.get("policy_min_uplift_usd_per_miner"), 0.25
        ),
        "energy_price_usd_per_kwh": max(
            0.0, _to_float(data.get("energy_price_usd_per_kwh"), 0.08)
        ),
        "hashprice_usd_per_ph_day": hashprice_usd_per_ph_day,
        "opex_usd_per_mwh": max(0.0, _to_float(data.get("opex_usd_per_mwh"), 8.0)),
        "capex_usd_per_mwh": max(0.0, _to_float(data.get("capex_usd_per_mwh"), 20.0)),
        "energy_price_schedule": _parse_hourly_price_schedule(
            data.get("energy_price_schedule_json")
        ),
        "curtailment_windows": _parse_curtailment_windows(
            data.get("curtailment_windows_json")
        ),
        "curtailment_penalty_multiplier": max(
            1.0,
            _to_float(
                data.get("curtailment_penalty_multiplier"),
                DEFAULT_CURTAILMENT_PENALTY_MULTIPLIER,
            ),
        ),
        "reward_per_th_hour_usd": reward_per_th_hour,
        "failure_cost_usd": max(
            0.0,
            _to_float(
                data.get("policy_failure_cost_usd"),
                DEFAULT_POLICY_FAILURE_COST_USD,
            ),
        ),
        "horizon_hours": max(0.25, _to_float(data.get("policy_horizon_hours"), 1.0)),
        "risk_probability_horizon_hours": max(
            1.0, _to_float(data.get("risk_probability_horizon_hours"), 24.0)
        ),
        "timezone": _to_str(data.get("policy_timezone"), "UTC"),
    }


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value or "").strip()
        if not text:
            return datetime.now(timezone.utc)
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _hour_is_in_window(hour: int, start_hour: int, end_hour: int) -> bool:
    if start_hour == end_hour:
        return False
    if start_hour < end_hour:
        return start_hour <= hour < end_hour
    return hour >= start_hour or hour < end_hour


def _is_curtailment_hour(local_hour: int, windows: list[tuple[int, int]]) -> bool:
    return any(_hour_is_in_window(local_hour, start, end) for start, end in windows)


def _energy_price_for_row(row: Mapping[str, Any], cfg: Mapping[str, Any]) -> tuple[float, bool]:
    timezone_name = _to_str(cfg.get("timezone"), "UTC")
    try:
        tz = ZoneInfo(timezone_name)
    except Exception:
        tz = ZoneInfo("UTC")

    ts = _parse_timestamp(row.get("timestamp")).astimezone(tz)
    local_hour = ts.hour
    schedule = cfg.get("energy_price_schedule") or {}
    base_price = _to_float(
        schedule.get(local_hour),
        _to_float(cfg.get("energy_price_usd_per_kwh"), 0.08),
    )
    in_curtailment = _is_curtailment_hour(
        local_hour, cfg.get("curtailment_windows") or []
    )
    if in_curtailment:
        base_price *= _to_float(
            cfg.get("curtailment_penalty_multiplier"),
            DEFAULT_CURTAILMENT_PENALTY_MULTIPLIER,
        )
    return max(0.0, base_price), in_curtailment


def _event_codes_text(value: Any) -> str:
    return str(value or "").upper()


def baseline_policy_decision(row: Mapping[str, Any]) -> dict[str, Any]:
    """Mirror the legacy threshold-only decision policy."""
    score = _clamp(_to_float(row.get("risk_score"), 0.0), 0.0, 1.0)
    events = _event_codes_text(row.get("event_codes"))

    action = "CONTINUE"
    automation = False
    reason = "baseline_default"
    if score >= 0.85 or "OVERTEMP" in events:
        action = "PULL_FOR_MAINTENANCE"
        reason = "baseline_critical_risk_or_overtemp"
    elif 0.75 <= score < 0.85:
        action = "REBOOT"
        automation = True
        reason = "baseline_high_risk"
    elif 0.60 <= score < 0.75:
        action = "DOWNCLOCK"
        automation = True
        reason = "baseline_medium_high_risk"
    elif 0.35 <= score < 0.60:
        action = "WATCHLIST"
        reason = "baseline_watchlist"

    return {
        "action": action,
        "automation_triggered": automation,
        "reason": reason,
    }


def estimate_action_utility(
    row: Mapping[str, Any], action: str, cfg: Mapping[str, Any]
) -> dict[str, Any]:
    """Estimate one-step utility for an action under economics + risk assumptions."""
    effect = ACTION_EFFECTS.get(action, ACTION_EFFECTS["CONTINUE"])
    risk = _clamp(_to_float(row.get("risk_score"), 0.0), 0.0, 1.0)
    asic_temp = _to_float(row.get("asic_temperature_c"), 75.0)
    if asic_temp >= 95.0:
        risk = _clamp(risk + 0.08, 0.0, 1.0)
    if asic_temp >= 100.0:
        risk = _clamp(risk + 0.10, 0.0, 1.0)

    power_w = max(0.0, _to_float(row.get("asic_power_w"), 0.0))
    hashrate_ths = max(0.0, _to_float(row.get("asic_hashrate_ths"), 0.0))
    horizon_hours = max(0.25, _to_float(cfg.get("horizon_hours"), 1.0))
    risk_horizon_hours = max(
        1.0, _to_float(cfg.get("risk_probability_horizon_hours"), 24.0)
    )
    run_hours = max(0.0, horizon_hours - effect["downtime_hours"])

    energy_price, in_curtailment = _energy_price_for_row(row, cfg)
    hashprice_usd_per_ph_day = max(
        0.0,
        _to_float(
            cfg.get("hashprice_usd_per_ph_day"),
            DEFAULT_HASHPRICE_USD_PER_PH_DAY,
        ),
    )
    reward_default = (
        _reward_from_hashprice(hashprice_usd_per_ph_day)
        if hashprice_usd_per_ph_day > 0
        else DEFAULT_POLICY_REWARD_PER_TH_HOUR_USD
    )
    reward_per_th_hour = max(
        0.0,
        _to_float(cfg.get("reward_per_th_hour_usd"), reward_default),
    )
    opex_usd_per_mwh = max(0.0, _to_float(cfg.get("opex_usd_per_mwh"), 0.0))
    capex_usd_per_mwh = max(0.0, _to_float(cfg.get("capex_usd_per_mwh"), 0.0))
    failure_cost = max(
        0.0,
        _to_float(cfg.get("failure_cost_usd"), DEFAULT_POLICY_FAILURE_COST_USD),
    )

    adjusted_hashrate_ths = hashrate_ths * effect["hashrate_multiplier"]
    adjusted_power_w = power_w * effect["power_multiplier"]
    expected_energy_mwh = adjusted_power_w * run_hours / 1_000_000.0
    energy_cost_usd_per_mwh = energy_price * 1000.0
    all_in_cost_usd_per_mwh = energy_cost_usd_per_mwh + opex_usd_per_mwh + capex_usd_per_mwh

    implied_eff_w_per_th = (
        adjusted_power_w / adjusted_hashrate_ths if adjusted_hashrate_ths > 0 else None
    )
    revenue_usd_per_mwh = None
    if implied_eff_w_per_th and implied_eff_w_per_th > 0 and hashprice_usd_per_ph_day > 0:
        # Revenue formula from mining economics:
        # (Hashprice [$/PH/day] / Efficiency [W/TH]) -> [$/MWh], with unit normalization.
        revenue_usd_per_mwh = hashprice_usd_per_ph_day * 1000.0 / (24.0 * implied_eff_w_per_th)

    if revenue_usd_per_mwh is not None:
        expected_revenue = revenue_usd_per_mwh * expected_energy_mwh
    else:
        # Backward-compatible fallback if hashprice/efficiency inputs are unavailable.
        expected_revenue = adjusted_hashrate_ths * reward_per_th_hour * run_hours

    expected_energy_cost = expected_energy_mwh * energy_cost_usd_per_mwh
    expected_all_in_cost = expected_energy_mwh * all_in_cost_usd_per_mwh
    failure_horizon_scaler = min(1.0, horizon_hours / risk_horizon_hours)
    expected_failure_cost = (
        risk * effect["risk_multiplier"] * failure_cost * failure_horizon_scaler
    )
    intervention_cost = effect["intervention_cost_usd"]

    utility = (
        expected_revenue
        - expected_all_in_cost
        - expected_failure_cost
        - intervention_cost
    )
    return {
        "utility_usd": float(utility),
        "expected_revenue_usd": float(expected_revenue),
        "expected_energy_cost_usd": float(expected_energy_cost),
        "expected_all_in_cost_usd": float(expected_all_in_cost),
        "expected_failure_cost_usd": float(expected_failure_cost),
        "intervention_cost_usd": float(intervention_cost),
        "expected_energy_mwh": float(expected_energy_mwh),
        "revenue_usd_per_mwh": (
            float(revenue_usd_per_mwh) if revenue_usd_per_mwh is not None else None
        ),
        "all_in_cost_usd_per_mwh": float(all_in_cost_usd_per_mwh),
        "implied_efficiency_w_per_th": (
            float(implied_eff_w_per_th) if implied_eff_w_per_th is not None else None
        ),
        "energy_price_usd_per_kwh": float(energy_price),
        "in_curtailment_window": bool(in_curtailment),
    }


def _action_allowed(row: Mapping[str, Any], action: str) -> bool:
    score = _clamp(_to_float(row.get("risk_score"), 0.0), 0.0, 1.0)
    asic_temp = _to_float(row.get("asic_temperature_c"), 75.0)
    events = _event_codes_text(row.get("event_codes"))

    if score >= 0.97 or asic_temp >= 102.0:
        return action == "PULL_FOR_MAINTENANCE"
    if score >= 0.85 and action in {"CONTINUE", "WATCHLIST", "DOWNCLOCK"}:
        return False
    if score >= 0.75 and action in {"CONTINUE", "WATCHLIST"}:
        return False
    if (
        action == "REBOOT"
        and score < 0.70
        and asic_temp < 92.0
        and "OVERTEMP" not in events
    ):
        return False
    if action == "DOWNCLOCK" and score < 0.55 and asic_temp < 90.0:
        return False
    if asic_temp >= 99.0 and action in {"CONTINUE", "WATCHLIST"}:
        return False
    if "OVERTEMP" in events and action in {"CONTINUE", "WATCHLIST"}:
        return False
    if score < 0.25 and action in {"PULL_FOR_MAINTENANCE"}:
        return False
    return True


def optimize_policy_decision(
    row: Mapping[str, Any], cfg: Mapping[str, Any]
) -> dict[str, Any]:
    """Choose action maximizing expected utility under safety constraints."""
    best_action = "CONTINUE"
    best_utility = float("-inf")
    best_breakdown: dict[str, Any] = {}

    for action in ACTIONS:
        if not _action_allowed(row, action):
            continue
        breakdown = estimate_action_utility(row, action, cfg)
        utility = float(breakdown["utility_usd"])
        if utility > best_utility:
            best_utility = utility
            best_action = action
            best_breakdown = breakdown

    automation = best_action in {"DOWNCLOCK", "REBOOT"}
    if best_action == "PULL_FOR_MAINTENANCE":
        automation = False

    reason = f"optimized_max_utility_{best_action.lower()}"
    return {
        "action": best_action,
        "automation_triggered": automation,
        "reason": reason,
        "expected_utility_usd": float(best_utility),
        "utility_breakdown": best_breakdown,
    }


def backtest_policy_uplift(
    rows: list[Mapping[str, Any]], cfg: Mapping[str, Any]
) -> dict[str, Any]:
    """Quantify optimized-policy utility uplift against baseline policy."""
    n = len(rows)
    if n == 0:
        return {
            "samples": 0,
            "baseline_avg_utility_usd": 0.0,
            "optimized_avg_utility_usd": 0.0,
            "avg_uplift_usd_per_miner": 0.0,
            "total_uplift_usd": 0.0,
            "baseline_action_mix": {},
            "optimized_action_mix": {},
            "baseline_automation_rate": 0.0,
            "optimized_automation_rate": 0.0,
            "minimum_required_uplift": _to_float(
                cfg.get("min_uplift_usd_per_miner"), 0.0
            ),
            "passed": True,
        }

    base_actions = Counter()
    opt_actions = Counter()
    base_automation = 0
    opt_automation = 0
    baseline_sum = 0.0
    optimized_sum = 0.0

    for row in rows:
        baseline = baseline_policy_decision(row)
        optimized = optimize_policy_decision(row, cfg)

        base_score = estimate_action_utility(row, baseline["action"], cfg)["utility_usd"]
        opt_score = estimate_action_utility(row, optimized["action"], cfg)["utility_usd"]

        baseline_sum += float(base_score)
        optimized_sum += float(opt_score)
        base_actions[baseline["action"]] += 1
        opt_actions[optimized["action"]] += 1
        base_automation += int(bool(baseline["automation_triggered"]))
        opt_automation += int(bool(optimized["automation_triggered"]))

    baseline_avg = baseline_sum / n
    optimized_avg = optimized_sum / n
    uplift = optimized_avg - baseline_avg
    min_required = _to_float(cfg.get("min_uplift_usd_per_miner"), 0.0)
    return {
        "samples": n,
        "baseline_avg_utility_usd": float(round(baseline_avg, 4)),
        "optimized_avg_utility_usd": float(round(optimized_avg, 4)),
        "avg_uplift_usd_per_miner": float(round(uplift, 4)),
        "total_uplift_usd": float(round((optimized_sum - baseline_sum), 4)),
        "baseline_action_mix": dict(base_actions),
        "optimized_action_mix": dict(opt_actions),
        "baseline_automation_rate": float(round(base_automation / n, 4)),
        "optimized_automation_rate": float(round(opt_automation / n, 4)),
        "minimum_required_uplift": float(round(min_required, 4)),
        "passed": bool(uplift >= min_required),
    }
