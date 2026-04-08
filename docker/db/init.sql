-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- API data sources (configurable external miner endpoints)
CREATE TABLE IF NOT EXISTS api_sources (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    url_template TEXT NOT NULL,
    auth_headers JSONB DEFAULT '{}',
    field_mapping JSONB DEFAULT '{}',
    polling_interval_minutes INTEGER DEFAULT 10,
    enabled BOOLEAN DEFAULT TRUE,
    last_fetched_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Raw telemetry from miners (hypertable)
CREATE TABLE IF NOT EXISTS telemetry (
    id BIGSERIAL NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    miner_id TEXT NOT NULL,
    source TEXT DEFAULT 'csv',
    asic_clock_mhz DOUBLE PRECISION,
    asic_voltage_v DOUBLE PRECISION,
    asic_hashrate_ths DOUBLE PRECISION,
    asic_temperature_c DOUBLE PRECISION,
    asic_power_w DOUBLE PRECISION,
    operating_mode TEXT DEFAULT 'normal',
    ambient_temperature_c DOUBLE PRECISION,
    chip_temp_max DOUBLE PRECISION,
    chip_temp_std DOUBLE PRECISION,
    bad_hash_count INTEGER DEFAULT 0,
    double_hash_count INTEGER DEFAULT 0,
    read_errors INTEGER DEFAULT 0,
    event_codes TEXT,
    expected_hashrate_ths DOUBLE PRECISION,
    PRIMARY KEY (id, timestamp)
);

SELECT create_hypertable('telemetry', 'timestamp', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_telemetry_miner_time ON telemetry (miner_id, timestamp DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_telemetry_miner_timestamp ON telemetry (miner_id, timestamp);

-- KPI-enriched telemetry (hypertable)
CREATE TABLE IF NOT EXISTS kpi_telemetry (
    id BIGSERIAL NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    miner_id TEXT NOT NULL,
    asic_clock_mhz DOUBLE PRECISION,
    asic_voltage_v DOUBLE PRECISION,
    asic_hashrate_ths DOUBLE PRECISION,
    asic_temperature_c DOUBLE PRECISION,
    asic_power_w DOUBLE PRECISION,
    operating_mode TEXT,
    ambient_temperature_c DOUBLE PRECISION,
    efficiency_j_per_th DOUBLE PRECISION,
    power_instability_index DOUBLE PRECISION,
    hashrate_deviation_pct DOUBLE PRECISION,
    true_efficiency_te DOUBLE PRECISION,
    failure_within_horizon INTEGER DEFAULT 0,
    chip_temp_max DOUBLE PRECISION,
    chip_temp_std DOUBLE PRECISION,
    bad_hash_count INTEGER DEFAULT 0,
    double_hash_count INTEGER DEFAULT 0,
    read_errors INTEGER DEFAULT 0,
    event_codes TEXT,
    expected_hashrate_ths DOUBLE PRECISION,
    PRIMARY KEY (id, timestamp)
);

SELECT create_hypertable('kpi_telemetry', 'timestamp', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_kpi_miner_time ON kpi_telemetry (miner_id, timestamp DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_kpi_miner_timestamp ON kpi_telemetry (miner_id, timestamp);

-- Hourly rollup for API analytics endpoints.
CREATE MATERIALIZED VIEW IF NOT EXISTS kpi_hourly_rollup
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', timestamp) AS bucket,
    miner_id,
    AVG(asic_hashrate_ths) AS avg_hashrate,
    AVG(asic_temperature_c) AS avg_temperature,
    AVG(asic_power_w) AS avg_power,
    AVG(asic_clock_mhz) AS avg_clock,
    AVG(asic_voltage_v) AS avg_voltage,
    AVG(efficiency_j_per_th) AS avg_efficiency_j_per_th,
    AVG(true_efficiency_te) AS avg_true_efficiency_te,
    MIN(operating_mode) AS operating_mode_sample,
    COUNT(*) AS samples
FROM kpi_telemetry
GROUP BY 1, 2
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_kpi_hourly_rollup_bucket_miner
ON kpi_hourly_rollup (bucket DESC, miner_id);

SELECT add_continuous_aggregate_policy(
    'kpi_hourly_rollup',
    start_offset => INTERVAL '30 days',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

SELECT add_retention_policy(
    'telemetry',
    drop_after => INTERVAL '180 days',
    if_not_exists => TRUE
);

SELECT add_retention_policy(
    'kpi_telemetry',
    drop_after => INTERVAL '180 days',
    if_not_exists => TRUE
);

-- ML risk predictions
CREATE TABLE IF NOT EXISTS risk_predictions (
    id BIGSERIAL PRIMARY KEY,
    predicted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    miner_id TEXT NOT NULL,
    risk_score DOUBLE PRECISION NOT NULL,
    risk_band TEXT NOT NULL,
    predicted_failure BOOLEAN DEFAULT FALSE,
    model_version TEXT DEFAULT 'v1'
);
CREATE INDEX IF NOT EXISTS idx_risk_miner_time ON risk_predictions (miner_id, predicted_at DESC);

-- Alerts
CREATE TABLE IF NOT EXISTS alerts (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    miner_id TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'warning',
    risk_score DOUBLE PRECISION,
    trigger_reason TEXT,
    message TEXT,
    recommended_action TEXT DEFAULT 'CONTINUE',
    automation_triggered BOOLEAN DEFAULT FALSE,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMPTZ,
    email_sent BOOLEAN DEFAULT FALSE,
    telegram_sent BOOLEAN DEFAULT FALSE
);

-- Application settings (key-value)
CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO app_settings (key, value) VALUES
    ('risk_threshold', '0.55'),
    ('alert_cooldown_hours', '1'),
    ('inference_lookback_hours', '24'),
    ('cooling_power_ratio', '0.24'),
    ('control_mode', 'advisory'),
    ('retrain_days', '30'),
    ('policy_optimizer_enabled', 'true'),
    ('automation_require_policy_backtest', 'true'),
    ('policy_min_uplift_usd_per_miner', '0.25'),
    ('energy_price_usd_per_kwh', '0.08'),
    ('hashprice_usd_per_ph_day', '55'),
    ('opex_usd_per_mwh', '8'),
    ('capex_usd_per_mwh', '20'),
    ('energy_price_schedule_json', '{}'),
    ('curtailment_windows_json', '[]'),
    ('curtailment_penalty_multiplier', '2.0'),
    ('policy_reward_per_th_hour_usd', '0.0022916667'),
    ('policy_failure_cost_usd', '300'),
    ('policy_horizon_hours', '1.0'),
    ('risk_probability_horizon_hours', '24'),
    ('policy_timezone', 'UTC'),
    ('smtp_host', ''),
    ('smtp_port', '587'),
    ('smtp_user', ''),
    ('smtp_password', ''),
    ('alert_from_email', ''),
    ('alert_to_emails', ''),
    ('telegram_bot_token', ''),
    ('telegram_chat_id', '')
ON CONFLICT (key) DO NOTHING;
