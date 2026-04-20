"""
Batch data ingestion engine.
Downloads OHLCV for multiple tickers × multiple timeframes with rate limiting.
Signals below MIN_CONFIDENCE_TO_STORE are discarded before hitting the database.
"""
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from backend.scraper.sources import download_ohlcv
from backend.scraper.ticker_list import get_all_tickers
from backend.storage.database import StockDatabase
from backend.analysis.indicators import calculate_all_indicators
from backend.analysis.divergence import detect_bullish_divergence, detect_hidden_bullish_divergence
from backend.analysis.elliott_abc import detect_abc_correction
from backend.analysis.accumulation import analyze_accumulation_distribution
from backend.analysis.trade_setup import calculate_trade_setup
from backend.scoring.confidence import calculate_confidence
from backend.scoring.ranker import rank_and_store_signals
from backend.config import (
    TIMEFRAMES,
    TIMEFRAME_PERIODS,
    MAX_CONCURRENT_DOWNLOADS,
    DOWNLOAD_DELAY_SECONDS,
    DIVERGENCE_INDICATORS,
    MIN_CONFIDENCE_TO_STORE,   # ← threshold baru
)

logger = logging.getLogger(__name__)


def scan_single_ticker(ticker: str, timeframes: list[str] | None = None):
    """
    Full pipeline untuk satu ticker.
    Setiap thread membuat instance StockDatabase sendiri (thread-safe).

    PERBAIKAN: db tidak lagi di-share antar thread — mencegah SQLite corruption.
    PERBAIKAN: sinyal di bawah MIN_CONFIDENCE_TO_STORE dibuang sebelum disimpan.
    """
    # Instance db baru per thread — FIXED thread-safety bug
    db = StockDatabase()
    db.initialize()

    if timeframes is None:
        timeframes = TIMEFRAMES

    all_signals = []

    try:
        for tf in timeframes:
            period = TIMEFRAME_PERIODS.get(tf, "1y")
            logger.info(f"Scanning {ticker} @ {tf}...")

            # 1. Download data
            df = download_ohlcv(ticker, tf, period)
            if df is None or len(df) < 30:
                logger.warning(f"Insufficient data for {ticker} @ {tf}, skipping")
                continue

            # Simpan OHLCV
            ticker_id = db.get_or_create_ticker(ticker)
            db.upsert_ohlcv(ticker_id, tf, df)

            # 2. Hitung indikator
            df_ind = df.copy()
            df_ind = df_ind.set_index("date")
            df_ind = calculate_all_indicators(df_ind)
            db.upsert_indicators(ticker_id, tf, df_ind)

            # 3. Deteksi sinyal
            signals_for_tf = []

            # 3a. Bullish Divergence
            for indicator in DIVERGENCE_INDICATORS:
                if indicator in df_ind.columns:
                    bd_signals = detect_bullish_divergence(df_ind, indicator=indicator)
                    for s in bd_signals:
                        s["ticker"] = ticker
                        s["timeframe"] = tf
                    signals_for_tf.extend(bd_signals)

            # 3b. Hidden Bullish Divergence
            for indicator in DIVERGENCE_INDICATORS:
                if indicator in df_ind.columns:
                    hbd_signals = detect_hidden_bullish_divergence(df_ind, indicator=indicator)
                    for s in hbd_signals:
                        s["ticker"] = ticker
                        s["timeframe"] = tf
                    signals_for_tf.extend(hbd_signals)

            # 3c. ABC Correction
            abc_signals = detect_abc_correction(df_ind)
            for s in abc_signals:
                s["ticker"] = ticker
                s["timeframe"] = tf
            signals_for_tf.extend(abc_signals)

            # 3d. Accumulation/Distribution (harian & mingguan saja)
            if tf in ("1d", "1wk"):
                accum_result = analyze_accumulation_distribution(df_ind)
                if accum_result and accum_result.get("phase") in ("accumulation", "distribution"):
                    accum_signal = {
                        "type": accum_result["phase"],
                        "ticker": ticker,
                        "timeframe": tf,
                        "date": df_ind.index[-1] if len(df_ind) > 0 else None,
                        "bar_index": len(df_ind) - 1,
                        "metadata": accum_result,
                    }
                    signals_for_tf.append(accum_signal)

                db.upsert_accum_dist(ticker_id, accum_result, df_ind)

            # 4. Hitung confidence score & trade setup untuk setiap sinyal
            for sig in signals_for_tf:
                sig["confidence_score"] = calculate_confidence(sig, df_ind, tf)
                
                # Tambahkan rekomendasi trading (Entry, SL, TP)
                trade_setup = calculate_trade_setup(sig, df_ind)
                if trade_setup:
                    if "metadata" not in sig:
                        sig["metadata"] = {}
                    sig["metadata"]["trade_setup"] = trade_setup

            # ── FILTER: buang sinyal di bawah threshold sebelum disimpan ──
            before = len(signals_for_tf)
            signals_for_tf = [
                s for s in signals_for_tf
                if s.get("confidence_score", 0) >= MIN_CONFIDENCE_TO_STORE
            ]
            dropped = before - len(signals_for_tf)
            if dropped:
                logger.debug(
                    f"{ticker} @ {tf}: {dropped} sinyal dibuang "
                    f"(score < {MIN_CONFIDENCE_TO_STORE})"
                )

            all_signals.extend(signals_for_tf)

    finally:
        db.close()

    # 5. Ranking & simpan
    if all_signals:
        # Buka koneksi baru untuk store (setelah semua TF selesai)
        db2 = StockDatabase()
        db2.initialize()
        try:
            rank_and_store_signals(db2, ticker, all_signals)
        finally:
            db2.close()
        logger.info(f"Found {len(all_signals)} qualified signals for {ticker}")

    return all_signals


def run_full_scan(
    category: str = "lq45",
    timeframes: list[str] | None = None,
    progress_callback=None,
):
    """
    Scan semua ticker dalam satu kategori secara paralel.
    Setiap worker thread punya koneksi DB sendiri.
    """
    tickers = get_all_tickers(category)
    total = len(tickers)
    logger.info(f"Starting full scan: {total} tickers, category={category}")

    results = {}
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADS) as executor:
        # FIXED: tidak lagi pass 'db' ke thread — setiap thread buat sendiri
        future_to_ticker = {}
        for i, ticker in enumerate(tickers):
            future = executor.submit(scan_single_ticker, ticker, timeframes)
            future_to_ticker[future] = ticker
            if (i + 1) % MAX_CONCURRENT_DOWNLOADS == 0:
                time.sleep(DOWNLOAD_DELAY_SECONDS)

        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                signals = future.result()
                results[ticker] = signals
            except Exception as e:
                logger.error(f"Error scanning {ticker}: {e}")
                results[ticker] = []
            finally:
                completed += 1
                if progress_callback:
                    progress_callback(completed, total, ticker)
                logger.info(f"Progress: {completed}/{total} ({ticker})")

    total_signals = sum(len(s) for s in results.values())
    logger.info(
        f"Full scan complete. {total_signals} qualified signals "
        f"(>= {MIN_CONFIDENCE_TO_STORE}) from {total} tickers."
    )
    return results