"""
Asynchronous data source abstraction layer.
Primary: yfinance library for robust OHLVC downloads with session management.
"""
import logging
import pandas as pd
import asyncio
import requests
import yfinance as yf
from datetime import datetime

logger = logging.getLogger(__name__)

# Global session no longer needed for yfinance 1.3.0+
# _session = requests.Session()

# Clear yfinance cache on start to prevent persistent session blocks
try:
    import shutil
    import os
    cache_path = os.path.expanduser('~/.cache/py-yfinance')
    if os.path.exists(cache_path):
        shutil.rmtree(cache_path)
        logger.info(f"🗑️ Cleared yfinance cache at {cache_path}")
except Exception as e:
    logger.debug(f"Could not clear yf cache: {e}")

async def download_ohlcv_async(ticker: str, timeframe: str, period: str) -> pd.DataFrame | None:
    """
    Download OHLCV data asynchronously using yfinance.
    Uses asyncio.to_thread to prevent blocking the event loop.
    """
    # Mapping internal timeframes to yfinance intervals
    interval_map = {
        "15m": "15m",
        "1h": "1h",
        "4h": "1h", # Resample from 1h
        "1d": "1d",
        "1wk": "1wk"
    }
    
    interval = interval_map.get(timeframe, "1d")
    
    try:
        # Wrap the synchronous yfinance call in a thread
        df = await asyncio.to_thread(
            yf.download,
            tickers=ticker,
            period=period,
            interval=interval,
            progress=False,
            auto_adjust=True,
            # session=_session  # yfinance 1.3.0+ handles its own curl_cffi session
        )
        
        if df is None or df.empty:
            logger.warning(f"No data returned for {ticker} @ {timeframe}")
            return None

        # ── Handle MultiIndex Columns ───────────────────────────────────────
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # ── Handle Index (Date/Datetime) ────────────────────────────────────
        # Reset index to move Date/Datetime from index to column
        df = df.reset_index()

        # Standardize all column names to lowercase
        df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
        
        # Identify the date column (it could be 'date', 'datetime', 'index', or something else)
        date_col = None
        possible_date_cols = ['date', 'datetime', 'timestamp', 'index']
        for col in possible_date_cols:
            if col in df.columns:
                date_col = col
                break
        
        if date_col:
            df = df.rename(columns={date_col: 'date'})
        else:
            # If still not found, try to find the first column that looks like a datetime
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df = df.rename(columns={col: 'date'})
                    date_col = 'date'
                    break
        
        if not date_col or 'date' not in df.columns:
            logger.error(f"Could not identify date column for {ticker}. Columns: {df.columns.tolist()}")
            return None
        
        # Ensure 'date' column is datetime and timezone-naive
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
        
        # Standardize OHLCV columns
        # Map common names to standard
        col_map = {
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'adj_close': 'close',
            'volume': 'volume'
        }
        df = df.rename(columns=col_map)
        
        # Resample to 4h if needed
        if timeframe == "4h":
            df = df.set_index('date')
            df = df.resample('4H').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna().reset_index()

        required = ["date", "open", "high", "low", "close", "volume"]
        
        # Verify all required columns exist
        missing = [c for c in required if c not in df.columns]
        if missing:
            logger.error(f"Missing columns for {ticker}: {missing}. Available: {df.columns.tolist()}")
            return None

        # Filter only required columns
        df = df[required].copy()
        df = df.sort_values("date").reset_index(drop=True)
        
        logger.info(f"Downloaded {len(df)} bars for {ticker} @ {timeframe} (yfinance)")
        return df

    except Exception as e:
        logger.error(f"yfinance download error for {ticker}: {e}")
        return None

async def get_ticker_info_async(ticker: str) -> dict:
    """
    Fetch basic info via yfinance library.
    """
    try:
        t = yf.Ticker(ticker)
        info = await asyncio.to_thread(lambda: t.info)
        
        if not info:
            return {"symbol": ticker, "name": "", "sector": "", "market_cap": 0}
            
        return {
            "symbol": ticker,
            "name": info.get("longName", info.get("shortName", ticker)),
            "sector": info.get("sector", "Unknown"),
            "market_cap": info.get("marketCap", 0),
            "volume": info.get("regularMarketVolume", 0),
            "avg_volume": info.get("averageDailyVolume3Month", 0)
        }
    except Exception as e:
        logger.error(f"Error fetching yfinance info for {ticker}: {e}")
    return {"symbol": ticker, "name": "", "sector": "", "market_cap": 0}
