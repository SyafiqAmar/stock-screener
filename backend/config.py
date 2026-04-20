"""
Global configuration for Stock Screener system
"""
import os
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
BACKEND_DIR = BASE_DIR / "backend"
FRONTEND_DIR = BASE_DIR / "frontend"
DATA_DIR = BASE_DIR / "data"
PARQUET_DIR = DATA_DIR / "parquet"
CSV_DIR = DATA_DIR / "csv"

DATA_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_DIR.mkdir(parents=True, exist_ok=True)
CSV_DIR.mkdir(parents=True, exist_ok=True)

# ── Database ───────────────────────────────────────────────────────────
DB_PATH = DATA_DIR / "stock_screener.db"

# ── Timeframes ─────────────────────────────────────────────────────────
TIMEFRAMES = ["15m", "1h", "4h", "1d", "1wk"]

TIMEFRAME_PERIODS = {
    "15m": "60d",
    "1h":  "730d",
    "4h":  "730d",
    "1d":  "2y",
    "1wk": "5y",
}

# ── Scraper Settings ───────────────────────────────────────────────────
MAX_CONCURRENT_DOWNLOADS = 5
DOWNLOAD_DELAY_SECONDS = 0.3
SCRAPE_DAILY_HOUR = 17
SCRAPE_DAILY_MINUTE = 5
SCRAPE_INTRADAY_INTERVAL_MINUTES = 15

# ── Analysis Settings ──────────────────────────────────────────────────# Divergence Settings
DIVERGENCE_LOOKBACK = 60
DIVERGENCE_MIN_PIVOT_DISTANCE = 5
DIVERGENCE_MAX_LAG = 3
DIVERGENCE_MIN_INDICATOR_DELTA = 3.0  # Min points for Ind (e.g. RSI 3.0 points)
DIVERGENCE_MIN_PRICE_PCT = 0.01       # Min 1% price diff for LL/HL
DIVERGENCE_INDICATORS = ["rsi_14", "macd_hist", "stoch_k"]

ABC_ZIGZAG_PCT = 5.0
ABC_LOOKBACK = 150
ABC_WAVE_B_RETRACE_MIN = 0.382
ABC_WAVE_B_RETRACE_MAX = 0.786
ABC_WAVE_C_EXTENSION_MIN = 0.618
ABC_WAVE_C_EXTENSION_MAX = 1.618

ACCUM_TREND_PERIOD = 20
ACCUM_VOLUME_AVG_PERIOD = 20

# ── Scoring Weights ────────────────────────────────────────────────────
SCORE_WEIGHTS = {
    "divergence_strength":      0.25,
    "multi_indicator_confirm":  0.20,
    "volume_confirmation":      0.15,
    "timeframe_weight":         0.15,
    "proximity":                0.10,
    "adl_alignment":            0.10,
    "fibonacci_precision":      0.05,
}

TIMEFRAME_SCORE_WEIGHTS = {
    "1wk": 1.0,
    "1d":  0.85,
    "4h":  0.70,
    "1h":  0.55,
    "15m": 0.40,
}

MULTI_TF_BONUS_MULTIPLIER = 1.5

# ── Confidence & Liquidity Filter ──────────────────────────────────────
# Hanya sinyal dengan score >= nilai ini yang akan masuk screener.
# 0.5 = probabilitas 50% ke atas.
# Naikkan ke 0.65 untuk hasil lebih selektif, turunkan ke 0.4 untuk lebih banyak sinyal.
MIN_CONFIDENCE_THRESHOLD = 0.5

# Sinyal di bawah threshold ini tidak disimpan ke database sama sekali
# (hemat storage, bersihkan noise sejak awal)
MIN_CONFIDENCE_TO_STORE = 0.4

# Batas likuiditas harian agar emiten muncul di dashboard (jumlah lembar saham)
# 1.000.000 adalah standard untuk IDX saham liquid (LQ45/IDX30)
MIN_LIQUIDITY_VOLUME = 1000000
MIN_LIQUIDITY_AVG_VOLUME = 1000000

# ── Server ─────────────────────────────────────────────────────────────
API_HOST = "0.0.0.0"
API_PORT = 8000

# ── Notifications ──────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
EMAIL_SMTP_HOST = os.getenv("EMAIL_SMTP_HOST", "")
EMAIL_SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASS = os.getenv("EMAIL_PASS", "")

# ── Redis ──────────────────────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "")
CACHE_TTL_SECONDS = 300