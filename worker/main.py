"""AI Controller background worker — APScheduler jobs."""

import os
import threading
import time

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from src.logging_utils import configure_logging
from src.secret_store import validate_secret_store_configuration

logger = configure_logging("worker")

DATABASE_URL = os.getenv(
    "DATABASE_URL_SYNC",
    "postgresql://aicontroller:aicontroller00@db:5432/aicontroller",
)
DB_STARTUP_TIMEOUT_SECONDS = int(os.getenv("DB_STARTUP_TIMEOUT_SECONDS", "90"))
DB_STARTUP_RETRY_INTERVAL_SECONDS = float(
    os.getenv("DB_STARTUP_RETRY_INTERVAL_SECONDS", "2")
)
DB_POOL_SIZE = max(int(os.getenv("DB_POOL_SIZE", "5")), 1)
DB_MAX_OVERFLOW = max(int(os.getenv("DB_MAX_OVERFLOW", "5")), 0)
_ENGINE = None
_ENGINE_LOCK = threading.Lock()


def _get_engine():
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE
    with _ENGINE_LOCK:
        if _ENGINE is not None:
            return _ENGINE
        from sqlalchemy import create_engine

        _ENGINE = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            pool_size=DB_POOL_SIZE,
            max_overflow=DB_MAX_OVERFLOW,
        )
    return _ENGINE


def _dispose_engine() -> None:
    global _ENGINE
    if _ENGINE is not None:
        _ENGINE.dispose()
        _ENGINE = None


def _wait_for_db_ready() -> bool:
    from sqlalchemy import text

    deadline = time.time() + max(DB_STARTUP_TIMEOUT_SECONDS, 1)
    last_error = None
    engine = _get_engine()

    while time.time() < deadline:
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as exc:
            last_error = exc
            time.sleep(max(DB_STARTUP_RETRY_INTERVAL_SECONDS, 0.25))

    logger.error(
        "Database did not become ready within %ss: %s",
        DB_STARTUP_TIMEOUT_SECONDS,
        last_error,
    )
    return False


def job_kpi_pipeline():
    """Compute KPI fields for any new raw telemetry rows."""
    from worker.ml_jobs import run_kpi_job
    try:
        run_kpi_job(_get_engine())
    except Exception as exc:
        logger.error("KPI job failed: %s", exc)


def job_inference():
    """Score all miners and write predictions + alerts."""
    from worker.ml_jobs import run_inference_job
    try:
        run_inference_job(_get_engine())
    except Exception as exc:
        logger.error("Inference job failed: %s", exc)


def job_fetch_sources():
    """Poll all enabled external API sources."""
    from worker.fetcher import run_fetch_job
    try:
        run_fetch_job(_get_engine())
    except Exception as exc:
        logger.error("Fetch job failed: %s", exc)


def job_send_alerts():
    """Deliver pending email and Telegram alerts."""
    from worker.notifier import run_notify_job
    try:
        run_notify_job(_get_engine())
    except Exception as exc:
        logger.error("Notify job failed: %s", exc)


def job_automator():
    """Execute automated actions for flagged alerts."""
    from worker.automator import run_automator_job
    try:
        run_automator_job(_get_engine())
    except Exception as exc:
        logger.error("Automator job failed: %s", exc)


def job_retrain():
    """Nightly model retraining on recent KPI data."""
    from worker.ml_jobs import run_retrain_job
    try:
        run_retrain_job(_get_engine())
    except Exception as exc:
        logger.error("Retrain job failed: %s", exc)


def main():
    validate_secret_store_configuration()
    logger.info("Worker starting; waiting for DB readiness...")
    # Eagerly create engine before scheduling to prevent thread races
    _get_engine()
    if not _wait_for_db_ready():
        _dispose_engine()
        raise SystemExit(1)

    scheduler = BlockingScheduler(timezone="UTC")

    scheduler.add_job(job_kpi_pipeline,   IntervalTrigger(minutes=15), id="kpi",       replace_existing=True)
    scheduler.add_job(job_inference,       IntervalTrigger(minutes=15), id="inference",  replace_existing=True)
    scheduler.add_job(job_fetch_sources,   IntervalTrigger(minutes=10), id="fetch",      replace_existing=True)
    scheduler.add_job(job_send_alerts,     IntervalTrigger(minutes=5),  id="notify",     replace_existing=True)
    scheduler.add_job(job_automator, IntervalTrigger(minutes=5), id="automator", replace_existing=True)
    logger.info(
        "Automator scheduled; execution is gated at runtime by control_mode (DB/env)"
    )
    scheduler.add_job(job_retrain,         CronTrigger(hour=2, minute=0), id="retrain", replace_existing=True)

    # Run KPI + inference once immediately on startup
    logger.info("Running initial KPI + inference...")
    job_kpi_pipeline()
    job_inference()

    logger.info("Worker scheduler started.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Worker stopped.")
    finally:
        _dispose_engine()


if __name__ == "__main__":
    main()
