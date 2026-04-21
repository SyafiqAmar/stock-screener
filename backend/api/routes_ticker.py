"""
Per-ticker detail API routes (Asynchronous).
Trade setup (Entry, SL, TP) is exposed as top-level fields for frontend ease.
"""
from fastapi import APIRouter, Query, HTTPException
from backend.storage.database import StockDatabase
from backend.scraper.ticker_list import get_ticker_info
from backend.storage.export import export_to_csv # export_to_csv might still be sync, which is fine for one-off file writes

router = APIRouter()

@router.get("/list")
async def list_tickers(category: str = Query("lq45")):
    """List all available tickers (Async)."""
    # This might need refinement if 'all_idx' category is selected
    from backend.scraper.ticker_list import get_all_tickers_async
    db = StockDatabase()
    try:
        tickers = await get_all_tickers_async(category, db)
        result = [get_ticker_info(t) for t in tickers]
        return {"category": category, "count": len(result), "tickers": result}
    finally:
        await db.close()

@router.get("/{symbol}")
async def get_ticker_detail(symbol: str):
    """
    Get full detail for a single ticker: info + all signals + accum data (Async).
    """
    db = StockDatabase()
    try:
        info = get_ticker_info(symbol)
        raw_signals = await db.get_signals_for_ticker(symbol)
        accum = await db.get_accum_dist(symbol, limit=30)

        # Get latest price from OHLCV
        ohlcv = await db.get_ohlcv(symbol, "1d", limit=2)
        latest_price = None
        price_change = None
        
        if not ohlcv.empty and len(ohlcv) >= 2:
            latest_price = float(ohlcv.iloc[-1]["close"])
            prev_price = float(ohlcv.iloc[-2]["close"])
            price_change = round((latest_price - prev_price) / prev_price * 100, 2)
        elif not ohlcv.empty:
            latest_price = float(ohlcv.iloc[-1]["close"])

        # Flatten trade_setup and calculate distance %
        signals = []
        for sig in raw_signals:
            # additional_data is already a dict in the new DB layer
            additional_data = sig.get("additional_data", {}) or {}
            trade_setup = additional_data.get("trade_setup", {}) or {}

            # These fields are also available at top level in the new Schema, but for robustness:
            sig["entry"] = sig.get("entry") or trade_setup.get("entry")
            sig["stop_loss"] = sig.get("stop_loss") or trade_setup.get("stop_loss")
            sig["target_1"] = sig.get("target_1") or trade_setup.get("target_1")
            sig["target_2"] = sig.get("target_2") or trade_setup.get("target_2")

            if sig["entry"] and sig["stop_loss"] and sig["entry"] > 0:
                sig["sl_pct"] = round((sig["entry"] - sig["stop_loss"]) / sig["entry"] * 100, 2)
            
            if sig["entry"] and sig.get("target_1") and sig["entry"] > 0:
                sig["tp1_pct"] = round((sig["target_1"] - sig["entry"]) / sig["entry"] * 100, 2)
            
            signals.append(sig)

        return {
            "info": info,
            "latest_price": latest_price,
            "price_change_pct": price_change,
            "signals": signals,
            "signal_count": len(signals),
            "accumulation_history": accum,
        }
    finally:
        await db.close()

@router.get("/{symbol}/accum")
async def get_ticker_accum(symbol: str, limit: int = Query(60)):
    """Get accumulation/distribution history for a ticker (Async)."""
    db = StockDatabase()
    try:
        accum = await db.get_accum_dist(symbol, limit=limit)
        return {"symbol": symbol, "data": accum}
    finally:
        await db.close()

@router.get("/{symbol}/export")
async def export_ticker_csv(symbol: str, timeframe: str = Query("1d")):
    """Export ticker data to CSV (Async Wrapper)."""
    # Keep as sync unless export_to_csv is slow
    filepath = export_to_csv(symbol, timeframe)
    if filepath:
        return {"status": "success", "file": filepath}
    return {"status": "error", "message": "No data to export"}