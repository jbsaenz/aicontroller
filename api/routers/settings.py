"""Application settings CRUD endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import verify_token
from api.db import get_db
from api.schemas import SettingsOut, SettingsUpdate

router = APIRouter()


@router.get("/settings", response_model=SettingsOut)
async def get_settings(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    result = await db.execute(text("SELECT key, value FROM app_settings ORDER BY key"))
    rows = result.mappings().all()
    return SettingsOut(settings={r["key"]: r["value"] for r in rows})


@router.put("/settings")
async def update_settings(
    body: SettingsUpdate,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    for key, value in body.settings.items():
        await db.execute(
            text("""
                INSERT INTO app_settings (key, value, updated_at)
                VALUES (:k, :v, NOW())
                ON CONFLICT (key) DO UPDATE SET value = :v, updated_at = NOW()
            """),
            {"k": key, "v": value},
        )
    await db.commit()
    return {"status": "ok", "updated": list(body.settings.keys())}


@router.get("/settings/{key}")
async def get_setting(
    key: str,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    result = await db.execute(
        text("SELECT value FROM app_settings WHERE key = :k"),
        {"k": key},
    )
    row = result.mappings().first()
    if not row:
        return {"key": key, "value": None}
    return {"key": key, "value": row["value"]}
