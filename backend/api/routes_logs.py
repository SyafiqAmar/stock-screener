"""
API Routes for System & Scraper Logs.
"""
from fastapi import APIRouter, Query
from backend.storage.database import StockDatabase

router = APIRouter()

@router.get("/scraper")
async def get_scraper_logs(
    limit: int = Query(100, ge=1, le=1000),
    symbol: str = Query(None)
):
    """Fetch recent scraper logs for UI monitoring."""
    db = StockDatabase()
    try:
        logs = await db.get_scrape_logs(limit=limit, symbol=symbol)
        return {"status": "success", "logs": logs}
    finally:
        await db.close()
