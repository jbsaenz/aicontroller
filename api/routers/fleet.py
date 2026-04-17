"""Fleet status endpoints."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import verify_token
from api.db import get_db

router = APIRouter()


def _fleet_miners_cte() -> str:
    """Shared CTE that identifies known miners from predictions, rollups, or recent KPI data."""
    return """
        WITH seeded_miners AS (
            SELECT miner_id
            FROM risk_predictions
            UNION
            SELECT miner_id
            FROM kpi_hourly_rollup
        ),
        fallback_miners AS (
            SELECT DISTINCT miner_id
            FROM kpi_telemetry
            WHERE timestamp >= NOW() - INTERVAL '7 days'
        ),
        miners AS (
            SELECT miner_id
            FROM seeded_miners
            UNION ALL
            SELECT miner_id
            FROM fallback_miners
            WHERE NOT EXISTS (SELECT 1 FROM seeded_miners)
        )
    """


@router.get("/fleet")
async def get_fleet(
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    result = await db.execute(
        text(f"""
            {_fleet_miners_cte()}
            SELECT
                t.miner_id, t.last_seen,
                t.asic_hashrate_ths, t.asic_temperature_c,
                t.asic_power_w, t.asic_voltage_v,
                t.asic_clock_mhz, t.operating_mode,
                COALESCE(p.risk_score, 0.0) AS risk_score,
                COALESCE(p.risk_band, 'unknown') AS risk_band,
                p.predicted_at
            FROM miners m
            JOIN LATERAL (
                SELECT
                    tele.miner_id,
                    tele.timestamp AS last_seen,
                    tele.asic_hashrate_ths,
                    tele.asic_temperature_c,
                    tele.asic_power_w,
                    tele.asic_voltage_v,
                    tele.asic_clock_mhz,
                    tele.operating_mode
                FROM telemetry tele
                WHERE tele.miner_id = m.miner_id
                ORDER BY tele.timestamp DESC
                LIMIT 1
            ) t ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    rp.risk_score,
                    rp.risk_band,
                    rp.predicted_at
                FROM risk_predictions rp
                WHERE rp.miner_id = m.miner_id
                ORDER BY rp.predicted_at DESC
                LIMIT 1
            ) p ON TRUE
            ORDER BY COALESCE(p.risk_score, 0.0) DESC
            LIMIT :lim OFFSET :off
        """),
        {"lim": limit, "off": offset},
    )
    rows = result.mappings().all()
    return [dict(r) for r in rows]


@router.get("/fleet/summary")
async def get_fleet_summary(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    result = await db.execute(
        text(f"""
            {_fleet_miners_cte()}
            SELECT
                COUNT(*)                                       AS total_miners,
                ROUND(AVG(l.asic_hashrate_ths)::numeric, 2)   AS avg_hashrate,
                ROUND(AVG(l.asic_temperature_c)::numeric, 1)  AS avg_temperature,
                ROUND(AVG(l.asic_power_w)::numeric, 0)        AS avg_power,
                ROUND(SUM(l.asic_hashrate_ths)::numeric, 2)   AS total_hashrate,
                COUNT(CASE WHEN p.risk_band = 'critical' THEN 1 END) AS critical_count,
                COUNT(CASE WHEN p.risk_band = 'high'     THEN 1 END) AS high_risk_count,
                COUNT(CASE WHEN p.risk_band IN ('low','medium') THEN 1 END) AS healthy_count
            FROM miners m
            JOIN LATERAL (
                SELECT
                    tele.asic_hashrate_ths,
                    tele.asic_temperature_c,
                    tele.asic_power_w
                FROM telemetry tele
                WHERE tele.miner_id = m.miner_id
                ORDER BY tele.timestamp DESC
                LIMIT 1
            ) l ON TRUE
            LEFT JOIN LATERAL (
                SELECT rp.risk_band
                FROM risk_predictions rp
                WHERE rp.miner_id = m.miner_id
                ORDER BY rp.predicted_at DESC
                LIMIT 1
            ) p ON TRUE
        """)
    )
    row = result.mappings().first()
    return dict(row) if row else {}

