"""
Per-ticker detail API routes.
Trade setup (Entry, SL, TP) diekspos sebagai field top-level agar mudah diakses frontend.
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
    result = [get_ticker_info(t) for t in tickers]
    return {"category": category, "count": len(result), "tickers": result}


@router.get("/{symbol}")
def get_ticker_detail(symbol: str):
    """
    Get full detail for a single ticker: info + all signals + accum data.
    trade_setup diekspos sebagai field top-level di setiap signal untuk kemudahan frontend.
    """
    db = StockDatabase()
    db.initialize()

    info = get_ticker_info(symbol)
    raw_signals = db.get_signals_for_ticker(symbol)
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

    # Flatten trade_setup ke top-level setiap signal
    # Sehingga frontend cukup akses signal.entry, signal.stop_loss, signal.target_1, signal.target_2
    signals = []
    for sig in raw_signals:
        metadata = sig.get("metadata", {}) or {}
        trade_setup = metadata.get("trade_setup", {}) or {}

        sig["entry"] = trade_setup.get("entry")
        sig["stop_loss"] = trade_setup.get("stop_loss")
        sig["target_1"] = trade_setup.get("target_1")
        sig["target_2"] = trade_setup.get("target_2")
        sig["risk_reward_1"] = trade_setup.get("risk_reward_1")

        # Hitung % jarak SL dan TP dari entry untuk kemudahan display
        if sig["entry"] and sig["stop_loss"] and sig["entry"] > 0:
            sig["sl_pct"] = round((sig["entry"] - sig["stop_loss"]) / sig["entry"] * 100, 2)
        else:
            sig["sl_pct"] = None

        if sig["entry"] and sig["target_1"] and sig["entry"] > 0:
            sig["tp1_pct"] = round((sig["target_1"] - sig["entry"]) / sig["entry"] * 100, 2)
        else:
            sig["tp1_pct"] = None

        if sig["entry"] and sig["target_2"] and sig["entry"] > 0:
            sig["tp2_pct"] = round((sig["target_2"] - sig["entry"]) / sig["entry"] * 100, 2)
        else:
            sig["tp2_pct"] = None

        signals.append(sig)

    return {
        "info": info,
        "latest_price": latest_price,
        "price_change_pct": price_change,
        "signals": signals,
        "signal_count": len(signals),
        "accumulation_history": accum,
    }


@router.get("/{symbol}/trade-setup")
def get_trade_setup(symbol: str, timeframe: str = Query("1d")):
    """
    Endpoint khusus untuk mendapatkan trade setup (Entry/SL/TP)
    dari sinyal terbaik untuk ticker tertentu.
    Berguna untuk widget atau notifikasi Telegram.
    """
    db = StockDatabase()
    db.initialize()
    signals = db.get_signals_for_ticker(symbol)
    db.close()

    if not signals:
        return {"symbol": symbol, "setup": None, "message": "No active signals"}

    # Filter by timeframe jika diminta
    if timeframe:
        filtered = [s for s in signals if s.get("timeframe") == timeframe]
        if filtered:
            signals = filtered

    # Ambil sinyal dengan confidence score tertinggi
    best = max(signals, key=lambda s: s.get("confidence_score", 0))
    metadata = best.get("metadata", {}) or {}
    trade_setup = metadata.get("trade_setup", {})

    if not trade_setup:
        return {
            "symbol": symbol,
            "signal_type": best.get("signal_type"),
            "timeframe": best.get("timeframe"),
            "confidence": best.get("confidence_score"),
            "setup": None,
            "message": "Signal found but no trade setup calculated yet. Re-run scan.",
        }

    entry = trade_setup.get("entry")
    sl = trade_setup.get("stop_loss")
    tp1 = trade_setup.get("target_1")
    tp2 = trade_setup.get("target_2")

    return {
        "symbol": symbol,
        "signal_type": best.get("signal_type"),
        "timeframe": best.get("timeframe"),
        "confidence": best.get("confidence_score"),
        "setup": {
            "entry": entry,
            "stop_loss": sl,
            "target_1": tp1,
            "target_2": tp2,
            "risk_reward_1": trade_setup.get("risk_reward_1"),
            # Persentase jarak
            "sl_pct": round((entry - sl) / entry * 100, 2) if entry and sl else None,
            "tp1_pct": round((tp1 - entry) / entry * 100, 2) if entry and tp1 else None,
            "tp2_pct": round((tp2 - entry) / entry * 100, 2) if entry and tp2 else None,
            # Wave info untuk ABC
            "wave_targets": trade_setup.get("patterns"),
        },
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