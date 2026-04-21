"""
Bulk import IDX tickers from Excel file in Downloads folder.
Filters by listing board: Ekonomi Baru, Pengembangan, Utama.
(Asynchronous PostgreSQL version)
"""
import os
import sys
import logging
import asyncio
from pathlib import Path
import pandas as pd

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from backend.storage.database import StockDatabase

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Config
EXCEL_PATH = Path("/app/data/list-company.xlsx")
ALLOWED_BOARDS = ["Ekonomi Baru", "Pengembangan", "Utama"]

async def run_import():
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
    await db.initialize()

    count_processed = 0

    for idx, row in df_filtered.iterrows():
        symbol = str(row['Kode']).strip().upper()
        if not symbol.endswith('.JK'):
            symbol += '.JK'
        
        name = str(row['Nama Perusahaan']).strip()
        
        try:
            # get_or_create_ticker handles the logic of checking existence and inserting
            await db.get_or_create_ticker(symbol=symbol, name=name)
            count_processed += 1
            
            if count_processed % 50 == 0:
                logger.info(f"Processed {count_processed}/{total_filtered} tickers...")
                
        except Exception as e:
            logger.error(f"Error importing {symbol}: {e}")

    await db.close()
    logger.info(f"Import complete! Processed {count_processed} tickers.")

if __name__ == "__main__":
    asyncio.run(run_import())
