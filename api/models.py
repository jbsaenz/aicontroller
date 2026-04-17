"""SQLAlchemy ORM models matching the TimescaleDB schema."""

from datetime import datetime, timezone

from sqlalchemy import BigInteger, Boolean, Column, DateTime, Double, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB

from api.db import Base


class Telemetry(Base):
    __tablename__ = "telemetry"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    miner_id = Column(Text, nullable=False)
    source = Column(Text, default="csv")
    asic_clock_mhz = Column(Double)
    asic_voltage_v = Column(Double)
    asic_hashrate_ths = Column(Double)
    asic_temperature_c = Column(Double)
    asic_power_w = Column(Double)
    operating_mode = Column(Text, default="normal")
    ambient_temperature_c = Column(Double)
    chip_temp_max = Column(Double)
    chip_temp_std = Column(Double)
    bad_hash_count = Column(Integer, default=0)
    double_hash_count = Column(Integer, default=0)
    read_errors = Column(Integer, default=0)
    event_codes = Column(Text)
    expected_hashrate_ths = Column(Double)


class KpiTelemetry(Base):
    __tablename__ = "kpi_telemetry"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    miner_id = Column(Text, nullable=False)
    asic_clock_mhz = Column(Double)
    asic_voltage_v = Column(Double)
    asic_hashrate_ths = Column(Double)
    asic_temperature_c = Column(Double)
    asic_power_w = Column(Double)
    operating_mode = Column(Text)
    ambient_temperature_c = Column(Double)
    efficiency_j_per_th = Column(Double)
    power_instability_index = Column(Double)
    hashrate_deviation_pct = Column(Double)
    true_efficiency_te = Column(Double)
    failure_within_horizon = Column(Integer, default=0)

    # Added Hardware Telemetry
    chip_temp_max = Column(Double)
    chip_temp_std = Column(Double)
    bad_hash_count = Column(Integer, default=0)
    double_hash_count = Column(Integer, default=0)
    read_errors = Column(Integer, default=0)
    event_codes = Column(Text)
    expected_hashrate_ths = Column(Double)


class RiskPrediction(Base):
    __tablename__ = "risk_predictions"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    predicted_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    miner_id = Column(Text, nullable=False)
    risk_score = Column(Double, nullable=False)
    risk_band = Column(Text, nullable=False)
    predicted_failure = Column(Boolean, default=False)
    model_version = Column(Text, default="v1")


class Alert(Base):
    __tablename__ = "alerts"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    miner_id = Column(Text, nullable=False)
    severity = Column(Text, default="warning")
    risk_score = Column(Double)
    trigger_reason = Column(Text)
    message = Column(Text)
    recommended_action = Column(Text, default="CONTINUE")
    automation_triggered = Column(Boolean, default=False)
    resolved = Column(Boolean, default=False)
    resolved_at = Column(DateTime(timezone=True))
    email_sent = Column(Boolean, default=False)
    telegram_sent = Column(Boolean, default=False)


class ApiSource(Base):
    __tablename__ = "api_sources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Text, nullable=False)
    url_template = Column(Text, nullable=False)
    auth_headers = Column(JSONB, default=dict)
    field_mapping = Column(JSONB, default=dict)
    polling_interval_minutes = Column(Integer, default=10)
    enabled = Column(Boolean, default=True)
    last_fetched_at = Column(DateTime(timezone=True))
    fetch_failure_streak = Column(Integer, default=0, nullable=False)
    last_fetch_attempt_at = Column(DateTime(timezone=True))
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class AppSetting(Base):
    __tablename__ = "app_settings"

    key = Column(Text, primary_key=True)
    value = Column(Text, default="")
    updated_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
