"""AI Controller background worker — APScheduler jobs."""

import logging
import os
import sys
import time

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# ── logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("worker")

DATABASE_URL = os.getenv(
    "DATABASE_URL_SYNC",
    "postgresql://aicontroller:aicontroller00@db:5432/aicontroller",
)


def _get_engine():
    from sqlalchemy import create_engine
    return create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=5)


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
    logger.info("Worker starting — waiting for DB to be ready...")
    time.sleep(5)  # Brief wait for DB healthcheck

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


if __name__ == "__main__":
    main()
