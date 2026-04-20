"""
Engine for batch scraping stock volume and 3-month average volume.
Uses yfinance Tickers feature for speed and efficiency.
"""
import logging
from datetime import datetime
import yfinance as yf
from backend.storage.database import StockDatabase

logger = logging.getLogger(__name__)

def update_volume_batch(limit: int = 50):
    """
    1. Get next 50 tickers from DB (oldest/never updated)
    2. Fetch current volume and 3m average from Yahoo Finance
    3. Update database
    """
    db = StockDatabase()
    db.initialize()
    
    symbols = db.get_next_tickers_for_volume_update(limit=limit)
    if not symbols:
        logger.info("No tickers found for volume update")
        db.close()
        return

    logger.info(f"Updating volume for batch of {len(symbols)} tickers...")
    
    # yf.Tickers accepts space-separated symbols
    tickers_obj = yf.Tickers(" ".join(symbols))
    
    count_success = 0
    
    for symbol in symbols:
        try:
            # Use fast_info for lightweight access
            ticker = tickers_obj.tickers[symbol]
            info = ticker.fast_info
            
            # lastVolume: current day's volume
            # threeMonthAverageVolume: 3-month average volume
            volume = int(info.get("lastVolume", 0))
            avg_volume = int(info.get("threeMonthAverageVolume", 0))
            
            if volume > 0 or avg_volume > 0:
                db.update_ticker_volume(symbol, volume, avg_volume)
                count_success += 1
            else:
                logger.warning(f"No volume data for {symbol}, placeholder update")
                # Still update timestamp so it goes to end of queue
                db.update_ticker_volume(symbol, 0, 0)
                
        except Exception as e:
            logger.error(f"Error updating volume for {symbol}: {e}")
            # Optional: update timestamp anyway to prevent stuck ticker
            db.update_ticker_volume(symbol, 0, 0)

    db.close()
    logger.info(f"Batch volume update complete. Successfully updated: {count_success}/{len(symbols)}")

if __name__ == "__main__":
    # Test run
    logging.basicConfig(level=logging.INFO)
    update_volume_batch(5)
