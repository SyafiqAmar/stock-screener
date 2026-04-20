"""
Per-ticker detail API routes.
"""
from fastapi import APIRouter, Query
from backend.storage.database import StockDatabase
from backend.scraper.ticker_list import get_all_tickers, get_ticker_info
from backend.storage.export import export_to_csv

router = APIRouter()


@router.get("/list")
def list_tickers(category: str = Query("lq45")):
    """List all available tickers."""
    tickers = get_all_tickers(category)
    result = []
    for t in tickers:
        info = get_ticker_info(t)
        result.append(info)
    return {"category": category, "count": len(result), "tickers": result}


@router.get("/{symbol}")
def get_ticker_detail(symbol: str):
    """Get full detail for a single ticker: info + all signals + accum data."""
    db = StockDatabase()
    db.initialize()

    info = get_ticker_info(symbol)
    signals = db.get_signals_for_ticker(symbol)
    accum = db.get_accum_dist(symbol, limit=30)

    # Get latest price from OHLCV
    ohlcv = db.get_ohlcv(symbol, "1d", limit=2)
    latest_price = None
    price_change = None
    if not ohlcv.empty and len(ohlcv) >= 2:
        latest_price = float(ohlcv.iloc[-1]["close"])
        prev_price = float(ohlcv.iloc[-2]["close"])
        price_change = round((latest_price - prev_price) / prev_price * 100, 2)
    elif not ohlcv.empty:
        latest_price = float(ohlcv.iloc[-1]["close"])

    db.close()

    return {
        "info": info,
        "latest_price": latest_price,
        "price_change_pct": price_change,
        "signals": signals,
        "signal_count": len(signals),
        "accumulation_history": accum,
    }


@router.get("/{symbol}/accum")
def get_ticker_accum(symbol: str, limit: int = Query(60)):
    """Get accumulation/distribution history for a ticker."""
    db = StockDatabase()
    db.initialize()
    accum = db.get_accum_dist(symbol, limit=limit)
    db.close()
    return {"symbol": symbol, "data": accum}


@router.get("/{symbol}/export")
def export_ticker_csv(symbol: str, timeframe: str = Query("1d")):
    """Export ticker data to CSV and return the file path."""
    filepath = export_to_csv(symbol, timeframe)
    if filepath:
        return {"status": "success", "file": filepath}
    return {"status": "error", "message": "No data to export"}
