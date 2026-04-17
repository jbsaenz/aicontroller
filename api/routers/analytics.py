"""Analytics: correlations, trade-offs, anomalies, TE distribution."""

import os
import time
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import verify_token
from api.db import get_db

router = APIRouter()

NUMERIC_COLS = [
    "asic_clock_mhz", "asic_voltage_v", "asic_hashrate_ths",
    "asic_temperature_c", "asic_power_w",
]
KPI_COLS = NUMERIC_COLS + ["efficiency_j_per_th", "true_efficiency_te"]
ROLLUP_COLUMNS = {
    "asic_clock_mhz": "avg_clock",
    "asic_voltage_v": "avg_voltage",
    "asic_hashrate_ths": "avg_hashrate",
    "asic_temperature_c": "avg_temperature",
    "asic_power_w": "avg_power",
    "efficiency_j_per_th": "avg_efficiency_j_per_th",
    "true_efficiency_te": "avg_true_efficiency_te",
}
ANALYTICS_ROLLUP_SCHEMA_CACHE_SECONDS = max(
    0,
    int(os.getenv("ANALYTICS_ROLLUP_SCHEMA_CACHE_SECONDS", "300")),
)
# NOTE: This mutable module-level dict is safe under uvicorn's default
# single-process asyncio model because Python's GIL guarantees atomic dict
# updates at the bytecode level. If deploying with multiple workers
# (--workers N), each process gets its own copy — which is acceptable since
# the cache is just a performance optimization for a cheap DB catalogue check.
_ROLLUP_SCHEMA_EXISTS_CACHE = {
    "value": None,
    "expires_at": 0.0,
}


async def _has_kpi_rollup(db: AsyncSession) -> bool:
    now = time.monotonic()
    cached = _ROLLUP_SCHEMA_EXISTS_CACHE["value"]
    if cached is not None and now < _ROLLUP_SCHEMA_EXISTS_CACHE["expires_at"]:
        return bool(cached)

    result = await db.execute(
        text("SELECT to_regclass('public.kpi_hourly_rollup') IS NOT NULL")
    )
    exists = bool(result.scalar())
    _ROLLUP_SCHEMA_EXISTS_CACHE["value"] = exists
    _ROLLUP_SCHEMA_EXISTS_CACHE["expires_at"] = (
        now + ANALYTICS_ROLLUP_SCHEMA_CACHE_SECONDS
    )
    return exists


async def _rollup_has_rows_in_window(
    db: AsyncSession, start_dt: datetime, end_dt: datetime
) -> bool:
    result = await db.execute(
        text("""
            SELECT EXISTS(
                SELECT 1
                FROM kpi_hourly_rollup
                WHERE bucket >= :start_dt AND bucket <= :end_dt
                LIMIT 1
            )
        """),
        {"start_dt": start_dt, "end_dt": end_dt},
    )
    return bool(result.scalar())


async def _should_use_rollup(
    db: AsyncSession, start_dt: datetime, end_dt: datetime
) -> bool:
    if not await _has_kpi_rollup(db):
        return False
    return await _rollup_has_rows_in_window(db, start_dt, end_dt)


def _correlation_source_sql(use_rollup: bool) -> str:
    if use_rollup:
        select_cols = ", ".join(
            f"{ROLLUP_COLUMNS[col]} AS {col}" for col in KPI_COLS
        )
        return f"""
            SELECT {select_cols}
            FROM kpi_hourly_rollup
            WHERE bucket >= :start_dt AND bucket <= :end_dt
        """

    return """
        SELECT asic_clock_mhz, asic_voltage_v, asic_hashrate_ths,
               asic_temperature_c, asic_power_w,
               efficiency_j_per_th, true_efficiency_te
        FROM kpi_telemetry
        WHERE timestamp >= :start_dt AND timestamp <= :end_dt
    """


def _build_correlation_sql(use_rollup: bool) -> str:
    corr_exprs = []
    for left_col in KPI_COLS:
        for right_col in KPI_COLS:
            corr_exprs.append(
                f"ROUND(CORR({left_col}, {right_col})::numeric, 3) AS {_correlation_alias(left_col, right_col)}"
            )

    source_sql = _correlation_source_sql(use_rollup)
    return f"""
        WITH scoped AS (
            {source_sql}
        )
        SELECT {", ".join(corr_exprs)}
        FROM scoped
    """


def _correlation_alias(left_col: str, right_col: str) -> str:
    return f"corr__{left_col}__{right_col}"


def _build_anomaly_sql() -> str:
    stat_exprs = ", ".join(
        [
            f"AVG({col}) AS mean_{col}, STDDEV_POP({col}) AS std_{col}"
            for col in NUMERIC_COLS
        ]
    )
    unions = []
    for col in NUMERIC_COLS:
        unions.append(
            f"""
            SELECT
                s.miner_id,
                s.timestamp,
                '{col}' AS field,
                s.{col} AS value,
                ABS((s.{col} - st.mean_{col}) / NULLIF(st.std_{col}, 0)) AS z_score
            FROM scoped s
            CROSS JOIN stats st
            WHERE s.{col} IS NOT NULL
              AND st.std_{col} IS NOT NULL
              AND st.std_{col} > 0
            """
        )

    return f"""
        WITH scoped AS (
            SELECT miner_id, timestamp, asic_clock_mhz, asic_voltage_v,
                   asic_hashrate_ths, asic_temperature_c, asic_power_w
            FROM kpi_telemetry
            WHERE timestamp >= :start_dt AND timestamp <= :end_dt
        ),
        stats AS (
            SELECT {stat_exprs}
            FROM scoped
        ),
        outliers AS (
            {" UNION ALL ".join(unions)}
        )
        SELECT
            miner_id,
            timestamp,
            field,
            value,
            ROUND(z_score::numeric, 3) AS z_score,
            CASE WHEN z_score > 5 THEN 'critical' ELSE 'warning' END AS severity
        FROM outliers
        WHERE z_score > 3
        ORDER BY z_score DESC
        LIMIT 200
    """


ANOMALY_SQL = text(_build_anomaly_sql())


@router.get("/analytics/correlations")
async def get_correlations(
    hours: int = Query(168, ge=1, le=720),
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    end_dt = end_time or datetime.now(timezone.utc)
    start_dt = start_time or (end_dt - timedelta(hours=hours))
    use_rollup = await _should_use_rollup(db, start_dt, end_dt)

    result = await db.execute(
        text(_build_correlation_sql(use_rollup)),
        {"start_dt": start_dt, "end_dt": end_dt},
    )
    row = result.mappings().first()
    if row is None:
        return {"columns": KPI_COLS, "matrix": []}

    matrix = []
    has_values = False
    for left_col in KPI_COLS:
        matrix_row = []
        for right_col in KPI_COLS:
            value = row.get(_correlation_alias(left_col, right_col))
            if value is not None:
                has_values = True
                matrix_row.append(float(value))
            else:
                matrix_row.append(None)
        matrix.append(matrix_row)

    if not has_values:
        return {"columns": KPI_COLS, "matrix": []}

    return {
        "columns": KPI_COLS,
        "matrix": matrix,
    }


@router.get("/analytics/tradeoffs")
async def get_tradeoffs(
    hours: int = Query(168, ge=1, le=720),
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit: int = Query(2000, le=5000),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    end_dt = end_time or datetime.now(timezone.utc)
    start_dt = start_time or (end_dt - timedelta(hours=hours))
    use_rollup = await _should_use_rollup(db, start_dt, end_dt)

    if use_rollup:
        query = text("""
            SELECT miner_id,
                   avg_hashrate AS asic_hashrate_ths,
                   avg_temperature AS asic_temperature_c,
                   avg_power AS asic_power_w,
                   avg_clock AS asic_clock_mhz,
                   avg_voltage AS asic_voltage_v,
                   avg_true_efficiency_te AS true_efficiency_te,
                   operating_mode_sample AS operating_mode
            FROM kpi_hourly_rollup
            WHERE bucket >= :start_dt AND bucket <= :end_dt
            ORDER BY bucket DESC
            LIMIT :lim
        """)
    else:
        query = text("""
            SELECT miner_id, asic_hashrate_ths, asic_temperature_c,
                   asic_power_w, asic_clock_mhz, asic_voltage_v,
                   true_efficiency_te, operating_mode
            FROM kpi_telemetry
            WHERE timestamp >= :start_dt AND timestamp <= :end_dt
            ORDER BY timestamp DESC
            LIMIT :lim
        """)

    result = await db.execute(
        query,
        {"start_dt": start_dt, "end_dt": end_dt, "lim": limit},
    )
    rows = [dict(r) for r in result.mappings().all()]
    return rows


@router.get("/analytics/anomalies")
async def get_anomalies(
    hours: int = Query(24, ge=1, le=168),
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    end_dt = end_time or datetime.now(timezone.utc)
    start_dt = start_time or (end_dt - timedelta(hours=hours))
    result = await db.execute(
        ANOMALY_SQL,
        {"start_dt": start_dt, "end_dt": end_dt},
    )
    return [dict(r) for r in result.mappings().all()]


@router.get("/analytics/efficiency")
async def get_efficiency_distribution(
    hours: int = Query(168, ge=1, le=720),
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_token),
):
    end_dt = end_time or datetime.now(timezone.utc)
    start_dt = start_time or (end_dt - timedelta(hours=hours))
    use_rollup = await _should_use_rollup(db, start_dt, end_dt)

    if use_rollup:
        query = text("""
            SELECT miner_id,
                   AVG(avg_true_efficiency_te) AS avg_te,
                   AVG(avg_efficiency_j_per_th) AS avg_efficiency,
                   AVG(avg_hashrate) AS avg_hashrate
            FROM kpi_hourly_rollup
            WHERE bucket >= :start_dt AND bucket <= :end_dt
            GROUP BY miner_id
            ORDER BY avg_te DESC
        """)
    else:
        query = text("""
            SELECT miner_id, AVG(true_efficiency_te) AS avg_te,
                   AVG(efficiency_j_per_th) AS avg_efficiency,
                   AVG(asic_hashrate_ths) AS avg_hashrate
            FROM kpi_telemetry
            WHERE timestamp >= :start_dt AND timestamp <= :end_dt
            GROUP BY miner_id
            ORDER BY avg_te DESC
        """)

    result = await db.execute(
        query,
        {"start_dt": start_dt, "end_dt": end_dt},
    )
    return [dict(r) for r in result.mappings().all()]
