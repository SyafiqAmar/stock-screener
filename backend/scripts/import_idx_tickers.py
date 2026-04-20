"""
Bulk import IDX tickers from Excel file in Downloads folder.
Filters by listing board: Ekonomi Baru, Pengembangan, Utama.
"""
import os
import sys
import logging
from pathlib import Path
import pandas as pd

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from backend.storage.database import StockDatabase

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Config
EXCEL_PATH = Path(os.path.expanduser("~/Downloads/list-company.xlsx"))
ALLOWED_BOARDS = ["Ekonomi Baru", "Pengembangan", "Utama"]

def run_import():
    if not EXCEL_PATH.exists():
        logger.error(f"File not found: {EXCEL_PATH}")
        return

    logger.info(f"Reading {EXCEL_PATH}...")
    try:
        df = pd.read_excel(EXCEL_PATH)
    except Exception as e:
        logger.error(f"Failed to read Excel: {e}")
        return

    # Check columns
    required_cols = ['Kode', 'Nama Perusahaan', 'Papan Pencatatan']
    for col in required_cols:
        if col not in df.columns:
            logger.error(f"Missing required column: {col}")
            logger.info(f"Available columns: {df.columns.tolist()}")
            return

    # Filter by boards
    total_raw = len(df)
    df_filtered = df[df['Papan Pencatatan'].isin(ALLOWED_BOARDS)].copy()
    total_filtered = len(df_filtered)
    
    logger.info(f"Filtered {total_filtered} tickers from {total_raw} total (Boards: {ALLOWED_BOARDS})")

    db = StockDatabase()
    db.initialize()

    count_new = 0
    count_updated = 0

    for idx, row in df_filtered.iterrows():
        symbol = str(row['Kode']).strip().upper()
        if not symbol.endswith('.JK'):
            symbol += '.JK'
        
        name = str(row['Nama Perusahaan']).strip()
        board = str(row['Papan Pencatatan']).strip()
        
        # Check if exists
        conn = db._connect()
        existing = conn.execute("SELECT id FROM tickers WHERE symbol = ?", (symbol,)).fetchone()
        
        if existing:
            # Update existing
            conn.execute(
                "UPDATE tickers SET name = ?, board = ?, updated_at = ? WHERE symbol = ?",
                (name, board, pd.Timestamp.now().isoformat(), symbol)
            )
            count_updated += 1
        else:
            # Insert new
            conn.execute(
                "INSERT INTO tickers (symbol, name, board, updated_at) VALUES (?, ?, ?, ?)",
                (symbol, name, board, pd.Timestamp.now().isoformat())
            )
            count_new += 1
        
        if (idx + 1) % 100 == 0:
            conn.commit()
            logger.info(f"Processed {idx + 1} tickers...")

    conn.commit()
    db.close()
    
    logger.info(f"Import complete! New: {count_new}, Updated: {count_updated}")

if __name__ == "__main__":
    run_import()
