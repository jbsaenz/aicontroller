"""Alerts CRUD endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import verify_token
from api.db import get_db
from api.schemas import AlertOut

router = APIRouter()


@router.get("/alerts", response_model=list[AlertOut])
async def get_alerts(
    resolved: bool = Query(False),
    limit: int = Query(100, le=500),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    result = await db.execute(
        text("""
            SELECT id, created_at, miner_id, severity, risk_score,
                   trigger_reason, message, recommended_action,
                   automation_triggered, resolved, resolved_at,
                   email_sent, telegram_sent
            FROM alerts
            WHERE resolved = :resolved
            ORDER BY
                CASE severity WHEN 'critical' THEN 0 ELSE 1 END,
                created_at DESC
            LIMIT :lim
        """),
        {"resolved": resolved, "lim": limit},
    )
    return [dict(r) for r in result.mappings().all()]


@router.get("/alerts/history", response_model=list[AlertOut])
async def get_alert_history(
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    result = await db.execute(
        text("""
            SELECT id, created_at, miner_id, severity, risk_score,
                   trigger_reason, message, recommended_action,
                   automation_triggered, resolved, resolved_at,
                   email_sent, telegram_sent
            FROM alerts
            ORDER BY created_at DESC
            LIMIT :lim OFFSET :off
        """),
        {"lim": limit, "off": offset},
    )
    return [dict(r) for r in result.mappings().all()]


@router.post("/alerts/{alert_id}/resolve")
async def resolve_alert(
    alert_id: int,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    result = await db.execute(
        text("""
            UPDATE alerts
            SET resolved = TRUE, resolved_at = NOW()
            WHERE id = :aid
            RETURNING id
        """),
        {"aid": alert_id},
    )
    if result.first() is None:
        await db.rollback()
        raise HTTPException(status_code=404, detail="Alert not found")
    await db.commit()
    return {"status": "resolved", "id": alert_id}
