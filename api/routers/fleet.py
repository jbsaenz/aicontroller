"""Fleet status endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import verify_token
from api.db import get_db

router = APIRouter()


@router.get("/fleet")
async def get_fleet(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    result = await db.execute(
        text("""
            WITH latest_tele AS (
                SELECT DISTINCT ON (miner_id)
                    miner_id,
                    timestamp AS last_seen,
                    asic_hashrate_ths, asic_temperature_c,
                    asic_power_w, asic_voltage_v,
                    asic_clock_mhz, operating_mode
                FROM telemetry
                ORDER BY miner_id, timestamp DESC
            ),
            latest_pred AS (
                SELECT DISTINCT ON (miner_id)
                    miner_id, risk_score, risk_band, predicted_at
                FROM risk_predictions
                ORDER BY miner_id, predicted_at DESC
            )
            SELECT
                t.miner_id, t.last_seen,
                t.asic_hashrate_ths, t.asic_temperature_c,
                t.asic_power_w, t.asic_voltage_v,
                t.asic_clock_mhz, t.operating_mode,
                COALESCE(p.risk_score, 0.0) AS risk_score,
                COALESCE(p.risk_band, 'unknown') AS risk_band,
                p.predicted_at
            FROM latest_tele t
            LEFT JOIN latest_pred p ON t.miner_id = p.miner_id
            ORDER BY COALESCE(p.risk_score, 0.0) DESC
        """)
    )
    rows = result.mappings().all()
    return [dict(r) for r in rows]


@router.get("/fleet/summary")
async def get_fleet_summary(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    result = await db.execute(
        text("""
            WITH latest AS (
                SELECT DISTINCT ON (miner_id)
                    miner_id, asic_hashrate_ths,
                    asic_temperature_c, asic_power_w
                FROM telemetry
                ORDER BY miner_id, timestamp DESC
            ),
            pred AS (
                SELECT DISTINCT ON (miner_id)
                    miner_id, risk_band
                FROM risk_predictions
                ORDER BY miner_id, predicted_at DESC
            )
            SELECT
                COUNT(DISTINCT l.miner_id)                     AS total_miners,
                ROUND(AVG(l.asic_hashrate_ths)::numeric, 2)   AS avg_hashrate,
                ROUND(AVG(l.asic_temperature_c)::numeric, 1)  AS avg_temperature,
                ROUND(AVG(l.asic_power_w)::numeric, 0)        AS avg_power,
                ROUND(SUM(l.asic_hashrate_ths)::numeric, 2)   AS total_hashrate,
                COUNT(CASE WHEN p.risk_band = 'critical' THEN 1 END) AS critical_count,
                COUNT(CASE WHEN p.risk_band = 'high'     THEN 1 END) AS high_risk_count,
                COUNT(CASE WHEN p.risk_band IN ('low','medium') THEN 1 END) AS healthy_count
            FROM latest l
            LEFT JOIN pred p ON l.miner_id = p.miner_id
        """)
    )
    row = result.mappings().first()
    return dict(row) if row else {}
