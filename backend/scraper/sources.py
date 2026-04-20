"""
Data source abstraction layer.
Primary: yfinance for OHLCV data.
"""
import logging
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


def download_ohlcv(ticker: str, timeframe: str, period: str) -> pd.DataFrame | None:
    """
    Download OHLCV data for a single ticker at a specific timeframe.

    Args:
        ticker: e.g., 'BBCA.JK'
        timeframe: yfinance interval — '15m', '1h', '4h', '1d', '1wk'
        period: yfinance period  — '60d', '730d', '2y', '5y'

    Returns:
        DataFrame with columns [date, open, high, low, close, volume] or None on error.
    """
    try:
        # yfinance uses 'interval' not 'timeframe'
        # For 4h, yfinance doesn't have native support — we resample from 1h
        actual_interval = timeframe
        actual_period = period

        if timeframe == "4h":
            actual_interval = "1h"
            # Need enough 1h data to resample into 4h

        data = yf.download(
            ticker,
            period=actual_period,
            interval=actual_interval,
            auto_adjust=True,
            progress=False,
            timeout=30,
        )

        if data is None or data.empty:
            logger.warning(f"No data returned for {ticker} @ {timeframe}")
            return None

        # Flatten MultiIndex columns if present (yfinance 0.2.x)
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)

        # Rename to lowercase standard
        data.columns = [c.lower() for c in data.columns]

        # Ensure we have the required columns
        required = ["open", "high", "low", "close", "volume"]
        for col in required:
            if col not in data.columns:
                logger.warning(f"Missing column '{col}' for {ticker} @ {timeframe}")
                return None

        # Resample to 4h if needed
        if timeframe == "4h":
            data = _resample_to_4h(data)

        # Reset index so 'date' is a column
        data = data.reset_index()
        date_col = [c for c in data.columns if "date" in c.lower() or "datetime" in c.lower()]
        if date_col:
            data = data.rename(columns={date_col[0]: "date"})
        elif "index" in data.columns:
            data = data.rename(columns={"index": "date"})

        # Keep only standard columns
        data = data[["date", "open", "high", "low", "close", "volume"]]
        data = data.dropna(subset=["close"])
        data = data.sort_values("date").reset_index(drop=True)

        logger.info(f"Downloaded {len(data)} bars for {ticker} @ {timeframe}")
        return data

    except Exception as e:
        logger.error(f"Error downloading {ticker} @ {timeframe}: {e}")
        return None


def _resample_to_4h(df: pd.DataFrame) -> pd.DataFrame:
    """Resample 1h OHLCV data to 4h bars."""
    ohlcv_agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    resampled = df.resample("4h").agg(ohlcv_agg).dropna()
    return resampled


def get_ticker_info_yf(ticker: str) -> dict:
    """Fetch additional info for a ticker from Yahoo Finance."""
    try:
        t = yf.Ticker(ticker)
        info = t.info
        return {
            "symbol": ticker,
            "name": info.get("longName", info.get("shortName", "")),
            "sector": info.get("sector", ""),
            "market_cap": info.get("marketCap", 0),
            "currency": info.get("currency", "IDR"),
        }
    except Exception as e:
        logger.error(f"Error fetching info for {ticker}: {e}")
        return {"symbol": ticker, "name": "", "sector": "", "market_cap": 0}
