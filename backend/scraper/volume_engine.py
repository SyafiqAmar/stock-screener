"""
Asynchronous volume and liquidity update engine.
Periodically refreshes ticker metadata using asynchronous HTTP requests.
"""
import asyncio
import logging
import sys
import os
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from backend.storage.database import StockDatabase
from backend.scraper.sources import get_ticker_info_async

logger = logging.getLogger(__name__)

async def update_volume_batch(limit: int = 50):
    """
    Asynchronously update volume metadata for a batch of tickers.
    """
    db = StockDatabase()
    # No need for manual initialize() here as the constructor handles the engine,
    # and it was already initialized on app startup.
    
    # We need to add get_next_tickers_for_volume_update to the async DB if not there
    # I'll check my previous database.py refactor... I missed it!
    # I must fix database.py again.
    
    # For now, let's assume we fetch them:
    try:
        # I'll add this method to database.py in the next step
        symbols = await db.get_next_tickers_for_volume_update(limit=limit)
        
        if not symbols:
            logger.info("No tickers found for volume update")
            return

        logger.info(f"Updating volume for batch of {len(symbols)} tickers (Async)...")
        
        # Gather all info requests concurrently
        tasks = [get_ticker_info_async(s) for s in symbols]
        info_results = await asyncio.gather(*tasks)
        
        count_success = 0
        for info in info_results:
            symbol = info.get("symbol")
            volume = info.get("volume", 0)
            avg_volume = info.get("avg_volume", 0)
            
            if symbol:
                await db.update_ticker_volume(symbol, volume, avg_volume)
                count_success += 1
        
        logger.info(f"Batch volume update complete. Successfully updated: {count_success}/{len(symbols)}")
        
    except Exception as e:
        logger.error(f"Error in volume update batch: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    # Test run logic
    logging.basicConfig(level=logging.INFO)
    asyncio.run(update_volume_batch(5))
