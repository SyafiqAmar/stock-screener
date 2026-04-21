"""
Manual script to update volume for ALL tickers in the database at once.
Useful for first-time initialization so the screener isn't empty.
"""
import sys
import logging
import asyncio
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
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

async def update_all_volumes():
    db = StockDatabase()
    # No need to call initialize() as update_volume_batch handles it or uses existing engine
    
    logger.info("🚀 Starting manual volume update process...")
    
    batch_size = 50
    # We will keep updating in batches until no more "old" tickers are found
    # Or just run it a certain number of times for safety.
    
    # Ideally, we'd check count, but get_next_tickers_for_volume_update 
    # will naturally cycle through them.
    
    for i in range(20):  # Process up to 1000 tickers (20 batches * 50)
        logger.info(f"Processing batch {i+1}...")
        try:
            await update_volume_batch(limit=batch_size)
        except Exception as e:
            logger.error(f"Error in batch {i+1}: {e}")
        
        # Small delay between batches to be polite to Yahoo Finance
        await asyncio.sleep(1)

    await db.close()
    logger.info("✅ Batch update finished. Refresh your browser to see the results.")

if __name__ == "__main__":
    asyncio.run(update_all_volumes())
