"""Daily background scraper scheduler.

Runs a full scrape for the current season/rundle once per day so the
analysis pages always reflect the latest match results.

The job is registered on FastAPI startup and runs at SCRAPE_HOUR (default
noon, configurable via SCRAPE_HOUR env var).  After each successful scrape
it clears both the in-memory response cache and the DB metric_cache so
stale computations are not served.
"""

import os
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import Config
from .logging import get_logger

logger = get_logger(__name__)

# Hour of day (0-23) to run the daily scrape, default noon.
SCRAPE_HOUR: int = int(os.getenv("SCRAPE_HOUR", "12"))

_scheduler: AsyncIOScheduler | None = None


def _run_daily_scrape() -> None:
    """Synchronous job executed by the scheduler thread pool."""
    from .scraper import LLScraper
    from .database import get_connection
    from .cache import response_cache
    from .metrics.registry import MetricRegistry

    season = Config.DEFAULT_SEASON
    rundle = Config.DEFAULT_RUNDLE
    logger.info("=== Daily scrape starting: Season %d / %s ===", season, rundle)

    scraper = LLScraper()
    if not scraper.login():
        logger.error("Daily scrape: login failed — skipping")
        return

    try:
        result = scraper.scrape_full(
            season,
            rundle,
            include_standings=True,
            include_my_answers=True,
            include_match_results=True,
            include_match_details=True,
            include_profiles=False,   # profiles rarely change mid-season
            include_rundle_answers=True,
        )
        logger.info("Daily scrape counts: %s", result.counts)
        if result.errors:
            logger.warning("Daily scrape errors (%d): %s", len(result.errors), result.errors[:5])
    finally:
        scraper.logout()

    # Clear caches so the next page load reflects fresh data.
    cleared_mem = response_cache.clear()
    logger.info("Cleared %d in-memory cache entries", cleared_mem)

    with get_connection() as conn:
        cleared_db = MetricRegistry.clear_cache(conn)
        logger.info("Cleared %d DB metric cache entries", cleared_db)

    logger.info("=== Daily scrape complete at %s ===", datetime.now().isoformat())


def start_scheduler() -> None:
    """Create and start the APScheduler instance."""
    global _scheduler

    _scheduler = AsyncIOScheduler()
    _scheduler.add_job(
        _run_daily_scrape,
        trigger=CronTrigger(hour=SCRAPE_HOUR, minute=0),
        id="daily_scrape",
        name="Daily LL scrape",
        replace_existing=True,
        misfire_grace_time=3600,   # allow up to 1 h late start
    )
    _scheduler.start()
    logger.info(
        "Scheduler started — daily scrape at %02d:00 (Season %d / %s)",
        SCRAPE_HOUR,
        Config.DEFAULT_SEASON,
        Config.DEFAULT_RUNDLE,
    )


def stop_scheduler() -> None:
    """Gracefully shut down the scheduler."""
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
    _scheduler = None


def trigger_scrape_now() -> dict:
    """
    Kick off an immediate scrape (for the manual /api/scrape/trigger endpoint).
    Runs in a thread so it does not block the event loop.

    Returns a status dict immediately; the scrape continues in the background.
    """
    import threading

    t = threading.Thread(target=_run_daily_scrape, daemon=True, name="manual-scrape")
    t.start()
    return {
        "status": "started",
        "season": Config.DEFAULT_SEASON,
        "rundle": Config.DEFAULT_RUNDLE,
        "triggered_at": datetime.now().isoformat(),
    }
