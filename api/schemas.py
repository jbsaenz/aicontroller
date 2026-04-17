"""Pydantic schemas for API request/response validation."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


# ── Fleet ──────────────────────────────────────────────────────────────────
class MinerStatus(BaseModel):
    miner_id: str
    last_seen: datetime | None
    asic_hashrate_ths: float | None
    asic_temperature_c: float | None
    asic_power_w: float | None
    asic_voltage_v: float | None
    asic_clock_mhz: float | None
    operating_mode: str | None
    risk_score: float = 0.0
    risk_band: str = "unknown"
    predicted_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class FleetSummary(BaseModel):
    total_miners: int = 0
    avg_hashrate: float | None
    avg_temperature: float | None
    avg_power: float | None
    total_hashrate: float | None
    critical_count: int = 0
    high_risk_count: int = 0
    healthy_count: int = 0


# ── Alerts ─────────────────────────────────────────────────────────────────
class AlertOut(BaseModel):
    id: int
    created_at: datetime
    miner_id: str
    severity: str
    risk_score: float | None
    trigger_reason: str | None
    message: str | None
    recommended_action: str
    automation_triggered: bool
    resolved: bool
    resolved_at: datetime | None
    email_sent: bool
    telegram_sent: bool

    model_config = ConfigDict(from_attributes=True)


# ── Ingest ─────────────────────────────────────────────────────────────────
class IngestResult(BaseModel):
    rows_received: int
    rows_inserted: int
    miners_found: list[str]
    errors: list[str] = Field(default_factory=list)


class ApiSourceIn(BaseModel):
    name: str
    url_template: str
    auth_headers: dict[str, str] = Field(default_factory=dict)
    field_mapping: dict[str, str] = Field(default_factory=dict)
    polling_interval_minutes: int = 10
    enabled: bool = True


class ApiSourceOut(ApiSourceIn):
    id: int
    last_fetched_at: datetime | None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


# ── Settings ───────────────────────────────────────────────────────────────
class SettingsUpdate(BaseModel):
    settings: dict[str, str]


class SettingsOut(BaseModel):
    settings: dict[str, str]


# ── Analytics ──────────────────────────────────────────────────────────────
class CorrelationMatrix(BaseModel):
    columns: list[str]
    matrix: list[list[float | None]]


class ScatterPoint(BaseModel):
    x: float
    y: float
    miner_id: str
    operating_mode: str | None


class AnomalyRow(BaseModel):
    miner_id: str
    timestamp: datetime
    field: str
    value: float
    z_score: float
    severity: str
