"""
OHLCV and Indicators API for Charting (Asynchronous).
Used by Lightweight Charts in the frontend.
"""
from fastapi import APIRouter, Query, HTTPException
from backend.storage.database import StockDatabase
import numpy as np
import pandas as pd

router = APIRouter()

@router.get("/{symbol}")
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
            return {"status": "success", "data": []}
        
        # Format for Lightweight Charts: list of {time, open, high, low, close}
        # time should be unix timestamp (seconds). Use .astype(int) for cleaner conversion.
        df['time'] = df['date'].astype('int64') // 10**9
        
        # Drop 'date' column to avoid JSON serialization error for pd.Timestamp
        df = df.drop(columns=['date'])
        
        # Handle NaN values (if any)
        df = df.replace({np.nan: None})
        
        records = df.to_dict('records')
        return {"status": "success", "data": records}
    finally:
        await db.close()

@router.get("/{symbol}/indicators")
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
            return {"status": "success", "indicators": []}
        
        # Convert date to unix timestamp
        df['time'] = df['date'].astype('int64') // 10**9
        
        # Drop 'date' column to avoid JSON serialization error for pd.Timestamp
        df = df.drop(columns=['date'])
        
        # Handle NaN values - vital for indicators!
        df = df.replace({np.nan: None})
        
        # Format as list of dicts with 'time' and indicator values
        records = df.to_dict('records')
        return {"status": "success", "indicators": records}
    finally:
        await db.close()

@router.get("/{symbol}/signals")
async def get_chart_signals(
    symbol: str,
    timeframe: str = Query("1d")
):
    """Fetch signals for chart markers (Async)."""
    db = StockDatabase()
    try:
        signals = await db.get_signals_for_ticker(symbol)
        
        # Format signals for JSON serialization (datetime to string)
        formatted_signals = []
        for s in signals:
            if s.get('detected_at') and hasattr(s['detected_at'], 'isoformat'):
                s['detected_at'] = s['detected_at'].isoformat()
            if timeframe == s.get('timeframe'):
                formatted_signals.append(s)
                
        return {"status": "success", "signals": formatted_signals}
    finally:
        await db.close()
