import os
import json
import sqlite3
from datetime import datetime
import sys

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from backend.storage.database import StockDatabase

def test_metadata_nesting():
    db_path = "test_screener.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    
    db = StockDatabase(db_path)
    db.initialize()
    
    ticker_id = db.get_or_create_ticker("TEST")
    
    # Simulate signal with metadata (as created in ingestion.py)
    signal = {
        "type": "bullish_divergence",
        "ticker": "TEST",
        "timeframe": "1d",
        "date": datetime.now().isoformat(),
        "confidence_score": 0.85,
        "indicator": "rsi_14",
        "metadata": {
            "trade_setup": {
                "entry": 100,
                "stop_loss": 90,
                "target_1": 120
            }
        }
    }
    
    print("Storing signal...")
    db.store_signal(ticker_id, "TEST", signal)
    
    print("Retrieving signal...")
    results = db.get_signals_for_ticker("TEST")
    
    if not results:
        print("FAILED: No signal retrieved")
        return

    retrieved = results[0]
    meta = retrieved.get("metadata", {})
    
    print(f"Retrieved Metadata: {json.dumps(meta, indent=2)}")
    
    # Check if trade_setup is at the top level of metadata
    if "trade_setup" in meta:
        print("SUCCESS: trade_setup found at top level of metadata")
        if "entry" in meta["trade_setup"]:
            print(f"SUCCESS: entry value is {meta['trade_setup']['entry']}")
    elif "metadata" in meta and "trade_setup" in meta["metadata"]:
        print("FAILED: trade_setup is still double-nested under 'metadata' key")
    else:
        print("FAILED: trade_setup not found at all")

    # Clean up
    db.close()
    if os.path.exists(db_path):
        os.remove(db_path)

if __name__ == "__main__":
    test_metadata_nesting()
