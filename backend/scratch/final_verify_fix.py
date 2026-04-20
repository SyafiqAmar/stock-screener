from backend.storage.database import StockDatabase
from datetime import datetime

def test_signal_filtering():
    db = StockDatabase()
    db.initialize()
    
    # 1. Check current active signals (should be 0 or only fresh ones)
    signals = db.get_active_signals(max_age_days=7)
    print(f"Total Active Signals (last 7 days): {len(signals)}")
    
    bbca_signals = [s for s in signals if s['symbol'] == 'BBCA.JK']
    if not bbca_signals:
        print("SUCCESS: BBCA.JK stale signals (2024) are now hidden.")
    else:
        for s in bbca_signals:
            print(f"FAILED: Found signal for BBCA.JK from {s['detected_at']}")

    # 2. Verify all_idx count
    from backend.scraper.ticker_list import get_all_tickers
    all_tix = get_all_tickers("all_idx")
    print(f"Database Ticker Count (all_idx): {len(all_tix)}")

if __name__ == "__main__":
    test_signal_filtering()
