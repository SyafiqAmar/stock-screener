"""
Asynchronous batch data ingestion engine.
Uses asyncio for non-blocking concurrent downloads and analysis.
"""
import asyncio
import logging
import pandas as pd
from datetime import datetime

from backend.scraper.sources import download_ohlcv_async
from backend.scraper.ticker_list import get_all_tickers, get_all_tickers_async
from backend.storage.database import StockDatabase
from backend.analysis.indicators import calculate_all_indicators
from backend.analysis.divergence import detect_bullish_divergence, detect_hidden_bullish_divergence
from backend.analysis.elliott_abc import detect_abc_correction
from backend.analysis.accumulation import analyze_accumulation_distribution
from backend.analysis.trade_setup import calculate_trade_setup
from backend.notifications.telegram_bot import send_telegram_alert, format_signal_alert
from backend.scoring.confidence import calculate_confidence
from backend.scoring.ranker import rank_and_store_signals
from backend.config import (
    TIMEFRAMES,
    TIMEFRAME_PERIODS,
    MAX_CONCURRENT_DOWNLOADS,
    DOWNLOAD_DELAY_SECONDS,
    DIVERGENCE_INDICATORS,
    MIN_CONFIDENCE_TO_STORE,
)

logger = logging.getLogger(__name__)

async def scan_single_ticker(
    ticker: str, 
    db: StockDatabase, 
    timeframes: list[str] | None = None,
    silent: bool = False
):
    """
    Asynchronous full pipeline for a single ticker.
    :param silent: If True, do NOT send individual Telegram alerts during the scan.
    """
    if timeframes is None:
        timeframes = TIMEFRAMES

    all_signals = []
    success_count = 0

    try:
        ticker_id = await db.get_or_create_ticker(ticker)
        for tf in timeframes:
            period = TIMEFRAME_PERIODS.get(tf, "2y")
            logger.info(f"Scanning {ticker} @ {tf}...")

            # 1. Async Download
            df = await download_ohlcv_async(ticker, tf, period)
            if df is None or len(df) < 30:
                logger.warning(f"Insufficient data for {ticker} @ {tf}, skipping")
                await db.log_scrape_event(ticker, tf, "failed", "Insufficient data or yfinance error")
                continue

            success_count += 1

            # 2. Async Store OHLCV
            await db.upsert_ohlcv(ticker_id, tf, df)

            # Update ticker volume summary if daily timeframe
            if tf == "1d" and len(df) > 0:
                latest_vol = int(df['volume'].iloc[-1]) if pd.notna(df['volume'].iloc[-1]) else 0
                avg_vol = int(df['volume'].tail(20).mean()) if len(df) >= 20 else latest_vol
                await db.update_ticker_volume(ticker, latest_vol, avg_vol)

            # 3. Calculate Indicators (CPU bound, but synchronous here is okay for now)
            df_ind = df.copy()
            df_ind = df_ind.set_index("date")
            df_ind = calculate_all_indicators(df_ind)
            await db.upsert_indicators(ticker_id, tf, df_ind)

            # 4. Pattern Recognition
            signals_for_tf = []

            # 4a. Divergence detection
            for indicator in DIVERGENCE_INDICATORS:
                if indicator in df_ind.columns:
                    bd_signals = detect_bullish_divergence(df_ind, indicator=indicator)
                    for s in bd_signals:
                        s["ticker"] = ticker
                        s["timeframe"] = tf
                    signals_for_tf.extend(bd_signals)

                    hbd_signals = detect_hidden_bullish_divergence(df_ind, indicator=indicator)
                    for s in hbd_signals:
                        s["ticker"] = ticker
                        s["timeframe"] = tf
                    signals_for_tf.extend(hbd_signals)

            # 4b. ABC Correction
            abc_signals = detect_abc_correction(df_ind)
            for s in abc_signals:
                s["ticker"] = ticker
                s["timeframe"] = tf
            signals_for_tf.extend(abc_signals)

            # 4c. Accumulation/Distribution
            if tf in ("1d", "1wk"):
                accum_result = analyze_accumulation_distribution(df_ind)
                if accum_result and accum_result.get("phase") in ("accumulation", "distribution"):
                    accum_signal = {
                        "type": accum_result["phase"],
                        "ticker": ticker,
                        "timeframe": tf,
                        "date": df_ind.index[-1] if len(df_ind) > 0 else None,
                        "additional_data": accum_result,
                    }
                    signals_for_tf.append(accum_signal)

                await db.upsert_accum_dist(ticker_id, accum_result, df_ind)

            # 5. Handle Signals
            for sig in signals_for_tf:
                sig["confidence_score"] = calculate_confidence(sig, df_ind, tf)
                
                trade_setup = calculate_trade_setup(sig, df_ind)
                if trade_setup:
                    if "additional_data" not in sig: sig["additional_data"] = {}
                    sig["additional_data"]["trade_setup"] = trade_setup
                
                # Only send alerts for Bullish Divergence, Hidden Bullish, and ABC Correction
                is_target_signal = sig.get("type") in ("bullish_divergence", "hidden_bullish_divergence", "abc_correction")
                
                if sig["confidence_score"] >= 0.8 and not silent and is_target_signal:
                    try:
                        alert_msg = format_signal_alert(sig)
                        await send_telegram_alert(alert_msg)
                    except Exception as e:
                        logger.error(f"Failed to send Telegram alert: {e}")

            # Filter signals based on threshold
            signals_for_tf = [
                s for s in signals_for_tf
                if s.get("confidence_score", 0) >= MIN_CONFIDENCE_TO_STORE
            ]
            all_signals.extend(signals_for_tf)
            
            # 5.5 Log Success
            await db.log_scrape_event(ticker, tf, "success")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Async scan failed for {ticker}: {error_msg}")
        # Log generic failure for the ticker if anything crashed the whole process
        await db.log_scrape_event(ticker, "all", "failed", error_msg)
        return {"status": "failed", "error": error_msg}

    # 6. Rank & Store Champions
    if all_signals:
        await rank_and_store_signals(db, ticker, all_signals)
    
    return {"status": "success"}

async def run_full_scan_async(
    category: str = "lq45",
    timeframes: list[str] | None = None,
    progress_callback=None,
):
    """
    Main asynchronous orchestration for full market scan.
    """
    db = StockDatabase()
    await db.initialize()

    if category == "all_idx":
        tickers = await get_all_tickers_async(category, db=db)
    else:
        tickers = get_all_tickers(category)

    total = len(tickers)
    logger.info(f"Starting async full scan: {total} tickers, category={category}")

    # Use a Semaphore to avoid overwhelming Yahoo or DB
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    
    consecutive_errors = 0
    scan_aborted = False
    MAX_ERRORS_BEFORE_COOLING = 3
    MAX_ERRORS_BEFORE_ABORT = 5
    
    async def sem_scan(ticker):
        nonlocal consecutive_errors, scan_aborted
        if scan_aborted:
            return ticker, {"status": "aborted"}

        async with semaphore:
            # ── Incremental Check ──────────────────────────────
            last_date = await db.get_last_ohlcv_date(ticker, "1d")
            if last_date:
                # If last update is within 4 hours, skip
                if (datetime.utcnow() - last_date).total_seconds() < 4 * 3600:
                    return ticker, {"status": "skipped", "reason": "fresh"}

            # ── Error Cooling ──────────────────────────────────
            if consecutive_errors >= MAX_ERRORS_BEFORE_COOLING:
                logger.warning(f"❄️ Cooling down for 2 minutes due to consecutive errors (Current: {consecutive_errors})...")
                await asyncio.sleep(120)
                # We don't reset counter here yet, we wait for a success

            result = await scan_single_ticker(ticker, db, timeframes, silent=(category == "all_idx"))
            
            # Update error counter
            if not result or result.get("status") == "failed":
                consecutive_errors += 1
                if consecutive_errors >= MAX_ERRORS_BEFORE_ABORT:
                    scan_aborted = True
                    logger.error(f"🛑 CRITICAL: {consecutive_errors} consecutive errors detected. Aborting entire scan to prevent permanent IP blacklist.")
            else:
                consecutive_errors = 0

            # Add random jitter to delay
            import random
            jitter = DOWNLOAD_DELAY_SECONDS * random.uniform(0.7, 1.3)
            await asyncio.sleep(jitter)
            return ticker, result

    tasks = [sem_scan(ticker) for ticker in tickers]
    
    completed = 0
    results = {}
    
    # Process tasks as they complete
    for task in asyncio.as_completed(tasks):
        ticker, signals = await task
        results[ticker] = signals
        completed += 1
        if progress_callback:
            progress_callback(completed, total, ticker)
        logger.info(f"Progress: {completed}/{total} ({ticker})")

    await db.close()
    
    total_signals = sum(len(s) for s in results.values())
    logger.info(f"Async scan complete. {total_signals} qualified signals from {total} tickers.")
    return results

def run_full_scan(category="lq45", timeframes=None, callback=None):
    """Synchronous wrapper for scheduler/compat."""
    return asyncio.run(run_full_scan_async(category, timeframes, callback))