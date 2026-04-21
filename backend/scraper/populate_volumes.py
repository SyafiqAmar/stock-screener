"""
Quick utility to bulk-populate ticker volume and metadata.
Run this to satisfy the liquidity filters (>1,000,000).
"""
import asyncio
import logging
from backend.storage.database import StockDatabase
from backend.scraper.sources import get_ticker_info_async
from backend.scraper.ticker_list import get_all_tickers

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

async def populate_all_volumes(category: str = "lq45"):
    db = StockDatabase()
    try:
        await db.initialize()
        
        tickers = get_all_tickers(category)
        logger.info(f"🚀 Populating volumes for {len(tickers)} tickers in category: {category}")
        
        # Process in batches of 20 to avoid rate limiting
        batch_size = 20
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i+batch_size]
            logger.info(f"⏳ Processing batch {i//batch_size + 1}/{(len(tickers)//batch_size)+1}...")
            
            tasks = [get_ticker_info_async(symbol) for symbol in batch]
            results = await asyncio.gather(*tasks)
            
            count = 0
            for info in results:
                if info and info.get("volume") is not None:
                    # Map volume and avg_volume from Yahoo response
                    vol = info.get("volume", 0)
                    avg_vol = info.get("avg_volume", 0)
                    name = info.get("name", "")
                    sector = info.get("sector", "")
                    
                    # Update DB
                    await db.get_or_create_ticker(info["symbol"], name=name, sector=sector)
                    await db.update_ticker_volume(info["symbol"], vol, avg_vol)
                    count += 1
            
            logger.info(f"✅ Batch complete. Updated {count} tickers.")
            await asyncio.sleep(1) # Polite delay
            
        logger.info("🎉 All volumes populated!")
        
    except Exception as e:
        logger.error(f"❌ Population failed: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    import sys
    category = sys.argv[1] if len(sys.argv) > 1 else "lq45"
    asyncio.run(populate_all_volumes(category))
