"""
Export utilities — Parquet and CSV backup.
"""
import logging
import pandas as pd
from pathlib import Path

from backend.config import PARQUET_DIR, CSV_DIR
from backend.storage.database import StockDatabase

logger = logging.getLogger(__name__)


def export_to_parquet(symbol: str, timeframe: str, db: StockDatabase | None = None):
    """Export OHLCV data for a ticker to Parquet file."""
    if db is None:
        db = StockDatabase()
        db.initialize()

    df = db.get_ohlcv(symbol, timeframe, limit=99999)
    if df.empty:
        logger.warning(f"No data to export for {symbol} @ {timeframe}")
        return None

    filename = f"{symbol.replace('.', '_')}_{timeframe}.parquet"
    filepath = PARQUET_DIR / filename
    df.to_parquet(filepath, index=False, engine="pyarrow")
    logger.info(f"Exported {len(df)} rows to {filepath}")
    return str(filepath)


def export_to_csv(symbol: str, timeframe: str, db: StockDatabase | None = None):
    """Export OHLCV data for a ticker to CSV file."""
    if db is None:
        db = StockDatabase()
        db.initialize()

    df = db.get_ohlcv(symbol, timeframe, limit=99999)
    if df.empty:
        return None

    filename = f"{symbol.replace('.', '_')}_{timeframe}.csv"
    filepath = CSV_DIR / filename
    df.to_csv(filepath, index=False)
    logger.info(f"Exported {len(df)} rows to {filepath}")
    return str(filepath)


def export_signals_csv(db: StockDatabase | None = None) -> str:
    """Export all active signals to CSV."""
    if db is None:
        db = StockDatabase()
        db.initialize()

    signals = db.get_active_signals(limit=9999)
    if not signals:
        return ""

    df = pd.DataFrame(signals)
    filepath = CSV_DIR / "screening_results.csv"
    df.to_csv(filepath, index=False)
    logger.info(f"Exported {len(df)} signals to {filepath}")
    return str(filepath)
