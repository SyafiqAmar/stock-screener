"""
FastAPI main entry point — Stock Screener Server.
Run: python -m backend.main
"""
import sys
import os
import logging
from pathlib import Path

# Add project root to path so imports work
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import uvicorn

from backend.api.routes_screener import router as screener_router
from backend.api.routes_chart import router as chart_router
from backend.api.routes_ticker import router as ticker_router
from backend.api.websocket_handler import websocket_endpoint
from backend.scraper.scheduler import start_scheduler, stop_scheduler
from backend.storage.database import StockDatabase
from backend.config import API_HOST, API_PORT, FRONTEND_DIR

# ── Logging ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(project_root / "data" / "screener.log"), encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ── App ────────────────────────────────────────────────
app = FastAPI(
    title="📊 Stock Screener IDX",
    description="Local stock screening system for Bullish Divergence, Hidden Bullish Divergence, ABC Correction, and Accumulation/Distribution analysis.",
    version="1.0.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── API Routes ─────────────────────────────────────────
app.include_router(screener_router, prefix="/api/screener", tags=["Screener"])
app.include_router(chart_router, prefix="/api/chart", tags=["Chart"])
app.include_router(ticker_router, prefix="/api/ticker", tags=["Ticker"])

# ── WebSocket ──────────────────────────────────────────
app.websocket("/ws/live")(websocket_endpoint)

# ── Static Files (Frontend) ───────────────────────────
frontend_path = str(FRONTEND_DIR)
if os.path.isdir(frontend_path):
    app.mount("/static", StaticFiles(directory=frontend_path), name="static")


# ── Root → serve frontend ─────────────────────────────
@app.get("/")
async def root():
    index_path = FRONTEND_DIR / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))
    return {"message": "Stock Screener API is running. Visit /docs for API documentation."}


# ── Lifecycle ──────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    logger.info("🚀 Starting Stock Screener...")

    # Initialize database
    db = StockDatabase()
    db.initialize()
    db.close()
    logger.info("✅ Database initialized")

    # Start scheduler
    start_scheduler()
    logger.info("✅ Scheduler started")

    logger.info(f"🌐 Dashboard: http://localhost:{API_PORT}")
    logger.info(f"📚 API Docs:  http://localhost:{API_PORT}/docs")


@app.on_event("shutdown")
async def shutdown_event():
    stop_scheduler()
    logger.info("Stock Screener stopped")


# ── Backend __init__ ───────────────────────────────────
# Create __init__.py for backend package if it doesn't exist
init_path = Path(__file__).parent / "__init__.py"
if not init_path.exists():
    init_path.write_text("# Backend package\n")

if __name__ == "__main__":
    uvicorn.run(
        "backend.main:app",
        host=API_HOST,
        port=API_PORT,
        reload=True,
        log_level="info",
    )
