"""
OHLCV and Indicators API for Charting (Asynchronous).
Used by Lightweight Charts in the frontend.
"""
from fastapi import APIRouter, Query, HTTPException
from backend.storage.database import StockDatabase
import pandas as pd

router = APIRouter()

@router.get("/ohlcv/{symbol}")
async def get_chart_ohlcv(
    symbol: str,
    timeframe: str = Query("1d", description="15m, 1h, 4h, 1d, 1wk"),
    limit: int = Query(500, ge=10, le=2000)
):
    """Fetch OHLCV data for chart (Async)."""
    db = StockDatabase()
    try:
        df = await db.get_ohlcv(symbol, timeframe, limit=limit)
        if df.empty:
            return []
        
        # Format for Lightweight Charts: list of {time, open, high, low, close}
        # time should be unix timestamp (seconds)
        df['time'] = df['date'].astype('int64') // 10**9
        records = df[['time', 'open', 'high', 'low', 'close', 'volume']].to_dict('records')
        return records
    finally:
        await db.close()

@router.get("/indicators/{symbol}")
async def get_chart_indicators(
    symbol: str, 
    timeframe: str = Query("1d"),
    limit: int = Query(500)
):
    """Fetch indicator data for chart (Async)."""
    db = StockDatabase()
    try:
        df = await db.get_indicators(symbol, timeframe, limit=limit)
        if df.empty:
            return {}
        
        # Convert date to unix timestamp
        df['time'] = df['date'].astype('int64') // 10**9
        
        # Group indicators for easier consumption by frontend
        indicators = {
            "rsi": df[['time', 'rsi_14']].rename(columns={'rsi_14': 'value'}).to_dict('records'),
            "macd": df[['time', 'macd', 'macd_signal', 'macd_hist']].to_dict('records'),
            "stoch": df[['time', 'stoch_k', 'stoch_d']].to_dict('records'),
            "labels": df[['time']].to_dict('records')
        }
        return indicators
    finally:
        await db.close()
