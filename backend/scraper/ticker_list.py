"""
Master list of IDX (Bursa Efek Indonesia) stock tickers.
Supports synchronous and asynchronous retrieval.
"""
import json
import os
import logging
from pathlib import Path

# Constituents
LQ45_TICKERS = [
    "ACES.JK", "ADRO.JK", "AMRT.JK", "ANTM.JK", "ASII.JK",
    "BBCA.JK", "BBNI.JK", "BBRI.JK", "BBTN.JK", "BMRI.JK",
    "BRIS.JK", "BRPT.JK", "BUKA.JK", "CPIN.JK", "EMTK.JK",
    "ESSA.JK", "EXCL.JK", "GGRM.JK", "GOTO.JK", "HRUM.JK",
    "ICBP.JK", "INCO.JK", "INDF.JK", "INKP.JK", "INTP.JK",
    "ITMG.JK", "KLBF.JK", "MAPI.JK", "MDKA.JK", "MEDC.JK",
    "MIKA.JK", "PGAS.JK", "PGEO.JK", "PTBA.JK", "SMGR.JK",
    "SRTG.JK", "TBIG.JK", "TINS.JK", "TLKM.JK", "TOWR.JK",
    "TPIA.JK", "UNTR.JK", "UNVR.JK", "WSKT.JK",
]

IDX30_TICKERS = [
    "ADRO.JK", "AMRT.JK", "ANTM.JK", "ASII.JK", "BBCA.JK",
    "BBNI.JK", "BBRI.JK", "BBTN.JK", "BMRI.JK", "BRIS.JK",
    "BRPT.JK", "CPIN.JK", "EMTK.JK", "EXCL.JK", "GOTO.JK",
    "HRUM.JK", "ICBP.JK", "INCO.JK", "INDF.JK", "INKP.JK",
    "KLBF.JK", "MDKA.JK", "MEDC.JK", "PGAS.JK", "PTBA.JK",
    "SMGR.JK", "TLKM.JK", "TOWR.JK", "UNTR.JK", "UNVR.JK",
]

# Static metadata
TICKER_SECTORS = {
    "BBCA.JK": "Banking", "BBRI.JK": "Banking", "BMRI.JK": "Banking",
    "BBNI.JK": "Banking", "BBTN.JK": "Banking", "BRIS.JK": "Banking",
    "TLKM.JK": "Telco", "ASII.JK": "Automotive", "ADRO.JK": "Mining",
    "ICBP.JK": "Consumer", "INDF.JK": "Consumer", "UNVR.JK": "Consumer",
    # (Mapping truncated for brevity, but kept in concept)
}

TICKER_NAMES = {
    "BBCA.JK": "Bank Central Asia",
    "BBRI.JK": "Bank Rakyat Indonesia",
    "BMRI.JK": "Bank Mandiri",
    "BBNI.JK": "Bank Negara Indonesia",
    "TLKM.JK": "Telkom Indonesia",
    "ASII.JK": "Astra International",
}

def get_all_tickers(category: str = "lq45") -> list[str]:
    """Synchronous version for simple cases."""
    if category == "lq45": return sorted(LQ45_TICKERS)
    if category == "idx30": return sorted(IDX30_TICKERS)
    return sorted(LQ45_TICKERS)

async def get_all_tickers_async(category: str = "lq45", db=None) -> list[str]:
    """Asynchronous version with DB lookup support."""
    if category == "all_idx" and db:
        tickers_data = await db.get_all_tickers()
        return sorted([t["symbol"] for t in tickers_data])
    return get_all_tickers(category)

def get_ticker_info(symbol: str) -> dict:
    """Synchronous metadata lookup."""
    return {
        "symbol": symbol,
        "name": TICKER_NAMES.get(symbol, symbol.replace(".JK", "")),
        "sector": TICKER_SECTORS.get(symbol, "Unknown")
    }
