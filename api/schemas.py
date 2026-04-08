"""Pydantic schemas for API request/response validation."""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


# ── Fleet ──────────────────────────────────────────────────────────────────
class MinerStatus(BaseModel):
    miner_id: str
    last_seen: Optional[datetime]
    asic_hashrate_ths: Optional[float]
    asic_temperature_c: Optional[float]
    asic_power_w: Optional[float]
    asic_voltage_v: Optional[float]
    asic_clock_mhz: Optional[float]
    operating_mode: Optional[str]
    risk_score: float = 0.0
    risk_band: str = "unknown"
    predicted_at: Optional[datetime]

    model_config = ConfigDict(from_attributes=True)


class FleetSummary(BaseModel):
    total_miners: int = 0
    avg_hashrate: Optional[float]
    avg_temperature: Optional[float]
    avg_power: Optional[float]
    total_hashrate: Optional[float]
    critical_count: int = 0
    high_risk_count: int = 0
    healthy_count: int = 0


# ── Alerts ─────────────────────────────────────────────────────────────────
class AlertOut(BaseModel):
    id: int
    created_at: datetime
    miner_id: str
    severity: str
    risk_score: Optional[float]
    trigger_reason: Optional[str]
    message: Optional[str]
    recommended_action: str
    automation_triggered: bool
    resolved: bool
    resolved_at: Optional[datetime]
    email_sent: bool
    telegram_sent: bool

    model_config = ConfigDict(from_attributes=True)


# ── Ingest ─────────────────────────────────────────────────────────────────
class IngestResult(BaseModel):
    rows_received: int
    rows_inserted: int
    miners_found: List[str]
    errors: List[str] = Field(default_factory=list)


class ApiSourceIn(BaseModel):
    name: str
    url_template: str
    auth_headers: Dict[str, str] = Field(default_factory=dict)
    field_mapping: Dict[str, str] = Field(default_factory=dict)
    polling_interval_minutes: int = 10
    enabled: bool = True


class ApiSourceOut(ApiSourceIn):
    id: int
    last_fetched_at: Optional[datetime]
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


# ── Settings ───────────────────────────────────────────────────────────────
class SettingsUpdate(BaseModel):
    settings: Dict[str, str]


class SettingsOut(BaseModel):
    settings: Dict[str, str]


# ── Analytics ──────────────────────────────────────────────────────────────
class CorrelationMatrix(BaseModel):
    columns: List[str]
    matrix: List[List[Optional[float]]]


class ScatterPoint(BaseModel):
    x: float
    y: float
    miner_id: str
    operating_mode: Optional[str]


class AnomalyRow(BaseModel):
    miner_id: str
    timestamp: datetime
    field: str
    value: float
    z_score: float
    severity: str
