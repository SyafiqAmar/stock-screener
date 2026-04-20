"""
Chart data API routes.
Provides OHLCV + indicator data for frontend charting.
"""
from fastapi import APIRouter, Query
from backend.storage.database import StockDatabase
from backend.storage.cache import cache_get, cache_set

router = APIRouter()


@router.get("/{ticker}")
def get_chart_data(
    ticker: str,
    timeframe: str = Query("1d", description="Timeframe: 15m, 1h, 4h, 1d, 1wk"),
    limit: int = Query(300, ge=50, le=2000),
):
    """
    Get OHLCV data for charting a specific ticker.
    Returns data formatted for TradingView Lightweight Charts.
    """
    cache_key = f"chart:{ticker}:{timeframe}:{limit}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    db = StockDatabase()
    db.initialize()
    df = db.get_ohlcv(ticker, timeframe, limit=limit)
    db.close()

    if df.empty:
        return {"ticker": ticker, "timeframe": timeframe, "data": [], "count": 0}

    # Format for Lightweight Charts (time as Unix timestamp)
    candles = []
    for _, row in df.iterrows():
        candles.append({
            "time": int(row["date"].timestamp()) if hasattr(row["date"], "timestamp") else str(row["date"]),
            "open": round(float(row["open"]), 2),
            "high": round(float(row["high"]), 2),
            "low": round(float(row["low"]), 2),
            "close": round(float(row["close"]), 2),
            "volume": int(row["volume"]),
        })

    response = {
        "ticker": ticker,
        "timeframe": timeframe,
        "count": len(candles),
        "data": candles,
    }

    cache_set(cache_key, response, ttl=60)
    return response


@router.get("/{ticker}/indicators")
def get_chart_indicators(
    ticker: str,
    timeframe: str = Query("1d"),
    limit: int = Query(300, ge=50, le=2000),
):
    """Get indicator data (RSI, MACD, Stoch, etc.) for chart overlays."""
    db = StockDatabase()
    db.initialize()
    df = db.get_indicators(ticker, timeframe, limit=limit)
    db.close()

    if df.empty:
        return {"ticker": ticker, "indicators": []}

    indicators = []
    for _, row in df.iterrows():
        entry = {
            "time": int(row["date"].timestamp()) if hasattr(row["date"], "timestamp") else str(row["date"]),
        }
        for col in ["rsi_14", "macd", "macd_signal", "macd_hist", "stoch_k", "stoch_d", "obv", "mfi", "adl"]:
            entry[col] = round(float(row[col]), 4) if row.get(col) is not None and str(row.get(col)) != "nan" else None
        indicators.append(entry)

    return {"ticker": ticker, "timeframe": timeframe, "indicators": indicators}


@router.get("/{ticker}/signals")
def get_chart_signals(ticker: str):
    """Get signal overlay data for chart markers (divergence lines, ABC waves)."""
    db = StockDatabase()
    db.initialize()
    signals = db.get_signals_for_ticker(ticker)
    db.close()

    return {"ticker": ticker, "signals": signals}
