"""
APScheduler-based job scheduler for periodic data scraping and analysis.
"""
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from backend.config import SCRAPE_DAILY_HOUR, SCRAPE_DAILY_MINUTE
from backend.scraper.ingestion import run_full_scan
from backend.scraper.volume_engine import update_volume_batch

logger = logging.getLogger(__name__)

# Global scheduler instance
_scheduler: BackgroundScheduler | None = None
_scan_status = {
    "is_running": False,
    "last_run": None,
    "last_result": None,
    "progress": {"completed": 0, "total": 0, "current_ticker": ""},
}


def get_scan_status() -> dict:
    """Get current scan status."""
    return _scan_status.copy()


def _update_progress(completed: int, total: int, ticker: str):
    """Callback for progress updates during scan."""
    _scan_status["progress"] = {
        "completed": completed,
        "total": total,
        "current_ticker": ticker,
    }


def _run_daily_scan():
    """Job: Run full daily scan (after market close)."""
    if _scan_status["is_running"]:
        logger.warning("Scan already running, skipping scheduled job")
        return

    _scan_status["is_running"] = True
    logger.info("Starting scheduled daily scan...")
    try:
        results = run_full_scan(
            category="lq45",
            timeframes=["1d", "1wk"],
            progress_callback=_update_progress,
        )
        _scan_status["last_result"] = {
            "total_tickers": len(results),
            "total_signals": sum(len(s) for s in results.values()),
        }
    except Exception as e:
        logger.error(f"Daily scan error: {e}")
    finally:
        _scan_status["is_running"] = False
        from datetime import datetime
        _scan_status["last_run"] = datetime.now().isoformat()


def run_manual_scan(category: str = "lq45", timeframes: list[str] | None = None):
    """Trigger a manual scan (called from API)."""
    if _scan_status["is_running"]:
        return {"status": "error", "message": "Scan already running"}

    _scan_status["is_running"] = True
    try:
        results = run_full_scan(
            category=category,
            timeframes=timeframes,
            progress_callback=_update_progress,
        )
        total_signals = sum(len(s) for s in results.values())
        _scan_status["last_result"] = {
            "total_tickers": len(results),
            "total_signals": total_signals,
        }
        from datetime import datetime
        _scan_status["last_run"] = datetime.now().isoformat()
        return {
            "status": "success",
            "total_tickers": len(results),
            "total_signals": total_signals,
        }
    except Exception as e:
        logger.error(f"Manual scan error: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        _scan_status["is_running"] = False


def start_scheduler():
    """Initialize and start the APScheduler."""
    global _scheduler
    if _scheduler is not None:
        logger.warning("Scheduler already running")
        return

    _scheduler = BackgroundScheduler()

    # Daily scan after market close (17:05 WIB)
    _scheduler.add_job(
        _run_daily_scan,
        trigger=CronTrigger(hour=SCRAPE_DAILY_HOUR, minute=SCRAPE_DAILY_MINUTE),
        id="daily_scan",
        name="Daily Full Scan",
        replace_existing=True,
    )

    # Batched Volume Update (Every 5 mins from 18:00 to 22:00 WIB)
    # This covers all ~900 tickers in about 1.5 hours
    _scheduler.add_job(
        update_volume_batch,
        trigger=CronTrigger(hour='18-21', minute='*/5'),
        id="volume_batch_update",
        name="Batched Volume Update (Post-Market)",
        args=[50], # Batch size
        replace_existing=True,
    )

    _scheduler.start()
    logger.info(f"Scheduler started. Daily scan at {SCRAPE_DAILY_HOUR}:{SCRAPE_DAILY_MINUTE:02d}")


def stop_scheduler():
    """Shutdown the scheduler gracefully."""
    global _scheduler
    if _scheduler:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        logger.info("Scheduler stopped")
