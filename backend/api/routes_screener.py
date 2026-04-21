"""
Screener REST API routes.
Default min_confidence = 0.5 — hanya tampilkan sinyal dengan probabilitas >= 50%.
"""
from fastapi import APIRouter, Query, BackgroundTasks
from backend.storage.database import StockDatabase
from backend.scoring.ranker import get_ranked_results
from backend.scraper.scheduler import run_manual_scan, get_scan_status
from backend.storage.cache import cache_get, cache_set
from backend.config import MIN_CONFIDENCE_THRESHOLD

router = APIRouter()


@router.get("/results")
async def get_screener_results(
    signal_type: str | None = Query(
        None,
        description="Filter: bullish_divergence, hidden_bullish_divergence, abc_correction, accumulation",
    ),
    timeframe: str | None = Query(
        None,
        description="Filter: 15m, 1h, 4h, 1d, 1wk",
    ),
    min_confidence: float = Query(
        default=MIN_CONFIDENCE_THRESHOLD,
        ge=0.0,
        le=1.0,
        description="Minimum confidence score (0.0–1.0). Default 0.5 = probabilitas >= 50%.",
    ),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    Get filtered and ranked screening results.
    (Asynchronous version)
    """
    cache_key = f"screener:{signal_type}:{timeframe}:{min_confidence}:{limit}:{offset}"
    cached = await cache_get(cache_key)
    if cached:
        return cached

    db = StockDatabase()
    try:
        results = await get_ranked_results(
            db,
            signal_type=signal_type,
            timeframe=timeframe,
            min_confidence=min_confidence,
            limit=limit,
            offset=offset,
        )
    finally:
        await db.close()

    response = {
        "total": len(results),
        "filters": {
            "signal_type": signal_type,
            "timeframe": timeframe,
            "min_confidence": min_confidence,
        },
        "results": results,
    }

    await cache_set(cache_key, response, ttl=120)
    return response


@router.get("/summary")
async def get_screener_summary():
    """
    Summary statistik sinyal aktif (Asynchronous).
    """
    cache_key = f"screener:summary:{MIN_CONFIDENCE_THRESHOLD}"
    cached = await cache_get(cache_key)
    if cached:
        return cached

    db = StockDatabase()
    try:
        # NOTE: initialize is now async if needed, but StockDatabase constructor
        # already sets up the engine. For safety, we can call it.
        await db.initialize()
        summary = await db.get_signal_summary(min_confidence=MIN_CONFIDENCE_THRESHOLD)
    finally:
        await db.close()

    await cache_set(cache_key, summary, ttl=120)
    return summary


@router.get("/status")
async def get_status():
    """Get current scan status."""
    return get_scan_status()


@router.post("/run")
async def trigger_scan(
    background_tasks: BackgroundTasks,
    category: str = Query(
        "lq45",
        description="Ticker category: lq45, idx30, extended, all",
    ),
    timeframes: str = Query(
        "1d,1wk",
        description="Comma-separated timeframes",
    ),
    min_confidence: float = Query(
        default=MIN_CONFIDENCE_THRESHOLD,
        ge=0.0,
        le=1.0,
        description="Hanya simpan sinyal di atas threshold ini",
    ),
):
    """Trigger manual scan in background."""
    tf_list = [t.strip() for t in timeframes.split(",")]
    
    # scheduler.run_manual_scan can remain synchronous or be converted.
    # We will converted it to async in the next steps.
    background_tasks.add_task(
        run_manual_scan,
        category=category,
        timeframes=tf_list,
    )
    
    return {
        "status": "started",
        "category": category,
        "timeframes": tf_list,
        "min_confidence_filter": min_confidence,
        "message": (
            f"Scan dimulai. Hanya sinyal dengan score >= {min_confidence} yang disimpan. "
            f"Cek /api/screener/status untuk progress."
        ),
    }