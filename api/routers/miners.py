"""Per-miner time-series and detail endpoints."""

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import verify_token
from api.db import get_db

router = APIRouter()


@router.get("/miners/{miner_id}")
async def get_miner_history(
    miner_id: str,
    hours: int = Query(24, ge=1, le=720),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    result = await db.execute(
        text("""
            SELECT timestamp, asic_clock_mhz, asic_voltage_v,
                   asic_hashrate_ths, asic_temperature_c, asic_power_w,
                   operating_mode
            FROM telemetry
            WHERE miner_id = :mid
              AND timestamp >= NOW() - INTERVAL '1 hour' * :hrs
            ORDER BY timestamp ASC
            LIMIT 5000
        """),
        {"mid": miner_id, "hrs": hours},
    )
    return [dict(r) for r in result.mappings().all()]


@router.get("/miners/{miner_id}/kpi")
async def get_miner_kpi(
    miner_id: str,
    hours: int = Query(24, ge=1, le=720),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    result = await db.execute(
        text("""
            SELECT timestamp, true_efficiency_te,
                   efficiency_j_per_th, power_instability_index,
                   hashrate_deviation_pct
            FROM kpi_telemetry
            WHERE miner_id = :mid
              AND timestamp >= NOW() - INTERVAL '1 hour' * :hrs
            ORDER BY timestamp ASC
            LIMIT 5000
        """),
        {"mid": miner_id, "hrs": hours},
    )
    return [dict(r) for r in result.mappings().all()]


@router.get("/miners/{miner_id}/risk")
async def get_miner_risk(
    miner_id: str,
    hours: int = Query(48, ge=1, le=720),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    result = await db.execute(
        text("""
            SELECT predicted_at, risk_score, risk_band, predicted_failure
            FROM risk_predictions
            WHERE miner_id = :mid
              AND predicted_at >= NOW() - INTERVAL '1 hour' * :hrs
            ORDER BY predicted_at ASC
        """),
        {"mid": miner_id, "hrs": hours},
    )
    return [dict(r) for r in result.mappings().all()]
