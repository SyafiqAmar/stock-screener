"""
Asynchronous job scheduler for periodic data scraping and analysis.
Uses AsyncIOScheduler to integrate with the FastAPI event loop.
"""
import logging
import asyncio
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from backend.config import SCRAPE_DAILY_HOUR, SCRAPE_DAILY_MINUTE
from backend.scraper.ingestion import run_full_scan_async
from backend.storage.database import StockDatabase
from backend.scoring.ranker import get_ranked_results
from backend.notifications.telegram_bot import send_top_signals_summary

logger = logging.getLogger(__name__)

# Global scheduler instance
_scheduler: AsyncIOScheduler | None = None
_scan_status = {
    "is_running": False,
    "last_run": None,
    "last_result": None,
    "progress": {"completed": 0, "total": 0, "current_ticker": ""},
}

def get_scan_status() -> dict:
    return _scan_status.copy()

def _update_progress(completed: int, total: int, ticker: str):
    """Callback for progress updates."""
    _scan_status["progress"] = {
        "completed": completed,
        "total": total,
        "current_ticker": ticker,
    }

async def _run_daily_scan():
    """Job: Run full daily scan (Async)."""
    if _scan_status["is_running"]:
        logger.warning("Scan already running, skipping scheduled daily job")
        return

    _scan_status["is_running"] = True
    logger.info("Starting scheduled daily scan (Async)...")
    try:
        results = await run_full_scan_async(
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
        _scan_status["last_run"] = datetime.now().isoformat()

async def _run_hourly_scan():
    """Job: Run hourly scan for the entire IDX market and send Top 5 summary."""
    if _scan_status["is_running"]:
        logger.warning("Scan already running, skipping scheduled hourly job")
        return

    _scan_status["is_running"] = True
    logger.info("Starting scheduled full market hourly scan (Async)...")
    
    db = StockDatabase()
    try:
        # Full Market Scan (all_idx) + Active Timeframes
        await run_full_scan_async(
            category="all_idx",
            timeframes=["1h", "4h", "1d"],
            progress_callback=_update_progress,
        )
        
        # After scan finishes, fetch the Top 5 ranked signals from DB
        # Only include Bullish Divergence, Hidden Bullish, and ABC Correction for Telegram
        top_signals = await get_ranked_results(
            db, 
            limit=5, 
            min_confidence=0.6,
            signal_types=["bullish_divergence", "hidden_bullish_divergence", "abc_correction"]
        )
        
        _scan_status["last_result"] = {
            "total_summary_signals": len(top_signals),
            "top_picks": [s.get("symbol") for s in top_signals]
        }

        # Send consolidated Telegram summary
        await send_top_signals_summary(top_signals)
        
    except Exception as e:
        logger.error(f"Hourly full market scan error: {e}")
    finally:
        await db.close()
        _scan_status["is_running"] = False
        _scan_status["last_run"] = datetime.now().isoformat()

async def run_manual_scan(category: str = "lq45", timeframes: list[str] | None = None):
    """Trigger a manual scan (Async)."""
    if _scan_status["is_running"]:
        return {"status": "error", "message": "Scan already running"}

    _scan_status["is_running"] = True
    db = StockDatabase()
    try:
        results = await run_full_scan_async(
            category=category,
            timeframes=timeframes,
            progress_callback=_update_progress,
        )
        
        # After scan finishes, fetch the Top 5 ranked signals from DB
        # Only include the specific types the user requested for Telegram
        top_signals = await get_ranked_results(
            db, 
            limit=5, 
            min_confidence=0.6,
            signal_types=["bullish_divergence", "hidden_bullish_divergence", "abc_correction"]
        )
        
        total_signals = sum(len(s) for s in results.values())
        _scan_status["last_result"] = {
            "total_tickers": len(results),
            "total_signals": total_signals,
            "top_picks": [s.get("symbol") for s in top_signals]
        }

        # Send consolidated Telegram summary
        await send_top_signals_summary(top_signals)
        
        _scan_status["last_run"] = datetime.now().isoformat()
        return {
            "status": "success",
            "total_tickers": len(results),
            "total_signals": total_signals,
            "top_picks": [s.get("symbol") for s in top_signals]
        }
    except Exception as e:
        logger.error(f"Manual scan error: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        await db.close()
        _scan_status["is_running"] = False

def start_scheduler():
    """Initialize and start the AsyncIOScheduler."""
    global _scheduler
    if _scheduler is not None:
        logger.warning("Scheduler already running")
        return

    _scheduler = AsyncIOScheduler()

    # Daily scan after market close
    _scheduler.add_job(
        _run_daily_scan,
        trigger=CronTrigger(hour=SCRAPE_DAILY_HOUR, minute=SCRAPE_DAILY_MINUTE),
        id="daily_scan",
        name="Daily Full Scan",
        replace_existing=True,
    )

    # Hourly scan
    _scheduler.add_job(
        _run_hourly_scan,
        trigger=IntervalTrigger(hours=1),
        id="hourly_scan",
        name="Hourly Full Scan",
        replace_existing=True,
    )

    _scheduler.start()
    logger.info(f"Async Scheduler started. Daily scan at {SCRAPE_DAILY_HOUR}:{SCRAPE_DAILY_MINUTE:02d}, plus Hourly scans.")

def stop_scheduler():
    """Shutdown the scheduler gracefully."""
    global _scheduler
    if _scheduler:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        logger.info("Scheduler stopped")
