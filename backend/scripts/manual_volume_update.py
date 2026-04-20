"""
Manual script to update volume for ALL tickers in the database at once.
Useful for first-time initialization so the screener isn't empty.
"""
import sys
import logging
import time
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from backend.storage.database import StockDatabase
from backend.scraper.volume_engine import update_volume_batch

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def update_all_volumes():
    db = StockDatabase()
    db.initialize()
    
    # Get all active tickers that have 0 volume (not yet updated)
    conn = db._connect()
    rows = conn.execute("SELECT COUNT(*) FROM tickers WHERE volume = 0 AND is_active = 1").fetchone()
    total_to_update = rows[0]
    
    if total_to_update == 0:
        logger.info("All tickers already have volume data. To force update, use the scheduler.")
        db.close()
        return

    logger.info(f"🚀 Starting manual volume update for {total_to_update} tickers...")
    
    batch_size = 50
    batches = (total_to_update // batch_size) + 1
    
    for i in range(batches):
        logger.info(f"Processing batch {i+1}/{batches}...")
        try:
            update_volume_batch(limit=batch_size)
        except Exception as e:
            logger.error(f"Error in batch {i+1}: {e}")
        
        # Small sleep between batches to be polite to Yahoo Finance
        if i < batches - 1:
            time.sleep(2)

    db.close()
    logger.info("✅ All tickers updated! Refresh your browser to see the results.")

if __name__ == "__main__":
    update_all_volumes()
