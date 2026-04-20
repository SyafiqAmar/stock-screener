"""
Master list of IDX (Bursa Efek Indonesia) stock tickers.
Format: SYMBOL.JK for Yahoo Finance compatibility.
"""

# ── LQ45 Constituents (Most Liquid) ───────────────────
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

# ── IDX30 Constituents ────────────────────────────────
IDX30_TICKERS = [
    "ADRO.JK", "AMRT.JK", "ANTM.JK", "ASII.JK", "BBCA.JK",
    "BBNI.JK", "BBRI.JK", "BBTN.JK", "BMRI.JK", "BRIS.JK",
    "BRPT.JK", "CPIN.JK", "EMTK.JK", "EXCL.JK", "GOTO.JK",
    "HRUM.JK", "ICBP.JK", "INCO.JK", "INDF.JK", "INKP.JK",
    "KLBF.JK", "MDKA.JK", "MEDC.JK", "PGAS.JK", "PTBA.JK",
    "SMGR.JK", "TLKM.JK", "TOWR.JK", "UNTR.JK", "UNVR.JK",
]

# ── Extended list: Kompas100 + popular tickers ────────
EXTENDED_TICKERS = [
    # Banking
    "BBYB.JK", "BGTG.JK", "BJTM.JK", "BNGA.JK", "BNII.JK",
    "BNLI.JK", "BTPS.JK", "MAYA.JK", "MEGA.JK", "NISP.JK",
    "PNBN.JK",
    # Consumer
    "ACES.JK", "AMRT.JK", "CLEO.JK", "GOOD.JK", "HMSP.JK",
    "ICBP.JK", "INDF.JK", "LPPF.JK", "MAPI.JK", "MYOR.JK",
    "RALS.JK", "SIDO.JK", "ULTJ.JK", "UNVR.JK",
    # Mining & Energy
    "ADRO.JK", "ANTM.JK", "BSSR.JK", "DSSA.JK", "ESSA.JK",
    "GEMS.JK", "HRUM.JK", "INCO.JK", "INDY.JK", "ITMG.JK",
    "MBAP.JK", "MDKA.JK", "MEDC.JK", "MPMX.JK", "PGAS.JK",
    "PGEO.JK", "PTBA.JK", "TINS.JK",
    # Infrastructure & Telco
    "EXCL.JK", "ISAT.JK", "JSMR.JK", "TBIG.JK", "TLKM.JK",
    "TOWR.JK", "WIKA.JK", "WSKT.JK",
    # Property & Construction
    "BSDE.JK", "CTRA.JK", "DMAS.JK", "LPKR.JK", "PWON.JK",
    "SMRA.JK", "WIKA.JK",
    # Industrial / Manufacture
    "ASII.JK", "AUTO.JK", "BRPT.JK", "GGRM.JK", "INKP.JK",
    "INTP.JK", "SMGR.JK", "SRTG.JK", "TKIM.JK", "TPIA.JK",
    "UNTR.JK",
    # Tech / Digital
    "BUKA.JK", "EMTK.JK", "GOTO.JK", "MIKA.JK",
    # Healthcare
    "HEAL.JK", "KLBF.JK", "MIKA.JK", "SILO.JK",
]

# ── Sector mapping ────────────────────────────────────
TICKER_SECTORS = {
    "BBCA.JK": "Banking", "BBRI.JK": "Banking", "BMRI.JK": "Banking",
    "BBNI.JK": "Banking", "BBTN.JK": "Banking", "BRIS.JK": "Banking",
    "MEGA.JK": "Banking", "BNGA.JK": "Banking", "PNBN.JK": "Banking",
    "TLKM.JK": "Telco", "EXCL.JK": "Telco", "ISAT.JK": "Telco",
    "ASII.JK": "Automotive", "AUTO.JK": "Automotive", "UNTR.JK": "Automotive",
    "ADRO.JK": "Mining", "ANTM.JK": "Mining", "PTBA.JK": "Mining",
    "INCO.JK": "Mining", "HRUM.JK": "Mining", "ITMG.JK": "Mining",
    "MDKA.JK": "Mining", "TINS.JK": "Mining", "MEDC.JK": "Energy",
    "PGAS.JK": "Energy", "ESSA.JK": "Energy",
    "ICBP.JK": "Consumer", "INDF.JK": "Consumer", "UNVR.JK": "Consumer",
    "GGRM.JK": "Consumer", "HMSP.JK": "Consumer", "KLBF.JK": "Healthcare",
    "MIKA.JK": "Healthcare", "SIDO.JK": "Healthcare",
    "INKP.JK": "Paper", "TKIM.JK": "Paper",
    "SMGR.JK": "Cement", "INTP.JK": "Cement",
    "CPIN.JK": "Poultry", "BRPT.JK": "Chemical", "TPIA.JK": "Chemical",
    "GOTO.JK": "Technology", "BUKA.JK": "Technology", "EMTK.JK": "Media",
    "TOWR.JK": "Tower", "TBIG.JK": "Tower",
    "CTRA.JK": "Property", "BSDE.JK": "Property", "SMRA.JK": "Property",
    "JSMR.JK": "Infrastructure", "WIKA.JK": "Infrastructure", "WSKT.JK": "Infrastructure",
}

# ── Ticker name mapping ──────────────────────────────
TICKER_NAMES = {
    "BBCA.JK": "Bank Central Asia",
    "BBRI.JK": "Bank Rakyat Indonesia",
    "BMRI.JK": "Bank Mandiri",
    "BBNI.JK": "Bank Negara Indonesia",
    "BBTN.JK": "Bank Tabungan Negara",
    "BRIS.JK": "Bank Syariah Indonesia",
    "TLKM.JK": "Telkom Indonesia",
    "ASII.JK": "Astra International",
    "UNVR.JK": "Unilever Indonesia",
    "ICBP.JK": "Indofood CBP",
    "INDF.JK": "Indofood Sukses Makmur",
    "ADRO.JK": "Adaro Energy",
    "ANTM.JK": "Aneka Tambang",
    "PTBA.JK": "Bukit Asam",
    "INCO.JK": "Vale Indonesia",
    "GOTO.JK": "GoTo Gojek Tokopedia",
    "BUKA.JK": "Bukalapak",
    "KLBF.JK": "Kalbe Farma",
    "GGRM.JK": "Gudang Garam",
    "SMGR.JK": "Semen Indonesia",
    "INKP.JK": "Indah Kiat Pulp",
    "MEDC.JK": "Medco Energi",
    "PGAS.JK": "Perusahaan Gas Negara",
    "CPIN.JK": "Charoen Pokphand",
    "UNTR.JK": "United Tractors",
    "MIKA.JK": "Mitra Keluarga Karyasehat",
    "EMTK.JK": "Elang Mahkota Teknologi",
    "HRUM.JK": "Harum Energy",
    "ITMG.JK": "Indo Tambangraya Megah",
    "MDKA.JK": "Merdeka Copper Gold",
    "EXCL.JK": "XL Axiata",
    "BRPT.JK": "Barito Pacific",
    "TPIA.JK": "Chandra Asri Petrochemical",
    "TOWR.JK": "Sarana Menara Nusantara",
    "TBIG.JK": "Tower Bersama",
    "CTRA.JK": "Ciputra Development",
    "BSDE.JK": "Bumi Serpong Damai",
    "JSMR.JK": "Jasa Marga",
}


def get_all_tickers(category: str = "lq45") -> list[str]:
    """
    Get list of tickers by category.
    Categories: 'lq45', 'idx30', 'extended', 'all'
    """
    if category == "lq45":
        return sorted(set(LQ45_TICKERS))
    elif category == "idx30":
        return sorted(set(IDX30_TICKERS))
    elif category == "extended":
        return sorted(set(EXTENDED_TICKERS))
    elif category == "all":
        # Combine all unique tickers
        all_tix = set(LQ45_TICKERS + IDX30_TICKERS + EXTENDED_TICKERS)
        return sorted(all_tix)
    elif category == "all_idx":
        # Dynamic: Fetch all active tickers from database
        from backend.storage.database import StockDatabase
        db = StockDatabase()
        db.initialize()
        tickers_data = db.get_all_tickers()
        db.close()
        return sorted([t["symbol"] for t in tickers_data])
    else:
        return sorted(set(LQ45_TICKERS))


def get_ticker_info(symbol: str) -> dict:
    """Get name and sector for a ticker symbol."""
    # Priority 1: Hardcoded map
    info = {
        "symbol": symbol,
        "name": TICKER_NAMES.get(symbol, ""),
        "sector": TICKER_SECTORS.get(symbol, "Unknown"),
    }
    
    # Priority 2: Database lookup if name is missing
    if not info["name"]:
        from backend.storage.database import StockDatabase
        db = StockDatabase()
        db.initialize()
        conn = db._connect()
        row = conn.execute("SELECT name, board FROM tickers WHERE symbol = ?", (symbol,)).fetchone()
        db.close()
        if row:
            info["name"] = row["name"]
            info["sector"] = row["board"] # Use board as fallback sector
        else:
            info["name"] = symbol.replace(".JK", "")
            
    return info
