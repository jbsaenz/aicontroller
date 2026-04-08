-- Add scalable analytics rollup to existing deployments.
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

-- Backfill recent data for immediate analytics usage.
CALL refresh_continuous_aggregate('kpi_hourly_rollup', NOW() - INTERVAL '30 days', NOW());
