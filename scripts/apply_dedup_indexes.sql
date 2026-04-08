-- Apply dedup constraints to existing AI Controller databases.
-- Run with:
-- psql "$DATABASE_URL_SYNC" -f scripts/apply_dedup_indexes.sql

BEGIN;

-- Keep newest telemetry row per (miner_id, timestamp)
WITH ranked AS (
    SELECT id, timestamp,
           ROW_NUMBER() OVER (
               PARTITION BY miner_id, timestamp
               ORDER BY id DESC
           ) AS rn
    FROM telemetry
)
DELETE FROM telemetry t
USING ranked r
WHERE t.id = r.id
  AND t.timestamp = r.timestamp
  AND r.rn > 1;

-- Keep newest KPI row per (miner_id, timestamp)
WITH ranked AS (
    SELECT id, timestamp,
           ROW_NUMBER() OVER (
               PARTITION BY miner_id, timestamp
               ORDER BY id DESC
           ) AS rn
    FROM kpi_telemetry
)
DELETE FROM kpi_telemetry k
USING ranked r
WHERE k.id = r.id
  AND k.timestamp = r.timestamp
  AND r.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS uq_telemetry_miner_timestamp
    ON telemetry (miner_id, timestamp);

CREATE UNIQUE INDEX IF NOT EXISTS uq_kpi_miner_timestamp
    ON kpi_telemetry (miner_id, timestamp);

COMMIT;
