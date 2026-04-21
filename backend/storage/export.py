"""
Asynchronous export utilities — CSV and Parquet backup.
"""
import logging
import pandas as pd
import asyncio
from pathlib import Path

from backend.config import PARQUET_DIR, CSV_DIR
from backend.storage.database import StockDatabase

logger = logging.getLogger(__name__)

async def export_to_parquet(symbol: str, timeframe: str, db: StockDatabase | None = None) -> str | None:
    """Export OHLCV data for a ticker to Parquet file (Async)."""
    own_db = False
    if db is None:
        db = StockDatabase()
        await db.initialize()
        own_db = True

    try:
        df = await db.get_ohlcv(symbol, timeframe, limit=99999)
        if df.empty:
            logger.warning(f"No data to export for {symbol} @ {timeframe}")
            return None

        filename = f"{symbol.replace('.', '_')}_{timeframe}.parquet"
        filepath = PARQUET_DIR / filename
        df.to_parquet(filepath, index=False, engine="pyarrow")
        logger.info(f"Exported {len(df)} rows to {filepath}")
        return str(filepath)
    finally:
        if own_db:
            await db.close()

async def export_to_csv(symbol: str, timeframe: str, db: StockDatabase | None = None) -> str | None:
    """Export OHLCV data for a ticker to CSV file (Async)."""
    own_db = False
    if db is None:
        db = StockDatabase()
        await db.initialize()
        own_db = True

    try:
        df = await db.get_ohlcv(symbol, timeframe, limit=99999)
        if df.empty:
            return None

        filename = f"{symbol.replace('.', '_')}_{timeframe}.csv"
        filepath = CSV_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Exported {len(df)} rows to {filepath}")
        return str(filepath)
    finally:
        if own_db:
            await db.close()

async def export_signals_csv(db: StockDatabase | None = None) -> str:
    """Export all active signals to CSV (Async)."""
    own_db = False
    if db is None:
        db = StockDatabase()
        await db.initialize()
        own_db = True

    try:
        # In current database.py, get_active_signals is async
        from backend.scoring.ranker import get_ranked_results
        signals = await get_ranked_results(db, limit=9999)
        
        if not signals:
            return ""

        df = pd.DataFrame(signals)
        filepath = CSV_DIR / "screening_results.csv"
        df.to_csv(filepath, index=False)
        logger.info(f"Exported {len(df)} signals to {filepath}")
        return str(filepath)
    finally:
        if own_db:
            await db.close()
