"""
SQLite database manager — schema creation, CRUD, and query operations.
"""
import json
import sqlite3
import logging
from datetime import datetime

import pandas as pd

from backend.config import DB_PATH, MIN_LIQUIDITY_VOLUME, MIN_LIQUIDITY_AVG_VOLUME

logger = logging.getLogger(__name__)


class StockDatabase:
    """SQLite database interface for the stock screener."""

    def __init__(self, db_path: str | None = None):
        self.db_path = str(db_path or DB_PATH)
        self.conn: sqlite3.Connection | None = None

    def _connect(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
        return self.conn

    def initialize(self):
        """Create all tables if they don't exist."""
        conn = self._connect()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS tickers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT UNIQUE NOT NULL,
                name TEXT DEFAULT '',
                sector TEXT DEFAULT '',
                board TEXT DEFAULT '',
                volume INTEGER DEFAULT 0,
                avg_volume INTEGER DEFAULT 0,
                volume_updated_at TEXT,
                is_active INTEGER DEFAULT 1,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS ohlcv (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker_id INTEGER REFERENCES tickers(id),
                timeframe TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                UNIQUE(ticker_id, timeframe, date)
            );

            CREATE TABLE IF NOT EXISTS indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker_id INTEGER REFERENCES tickers(id),
                timeframe TEXT NOT NULL,
                date TEXT NOT NULL,
                rsi_14 REAL,
                macd REAL,
                macd_signal REAL,
                macd_hist REAL,
                stoch_k REAL,
                stoch_d REAL,
                obv REAL,
                mfi REAL,
                adl REAL,
                UNIQUE(ticker_id, timeframe, date)
            );

            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker_id INTEGER REFERENCES tickers(id),
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                detected_at TEXT NOT NULL,
                confidence_score REAL DEFAULT 0,
                metadata TEXT DEFAULT '{}',
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS accum_dist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker_id INTEGER REFERENCES tickers(id),
                date TEXT NOT NULL,
                phase TEXT,
                adl_value REAL,
                obv_value REAL,
                mfi_value REAL,
                volume_ratio REAL,
                UNIQUE(ticker_id, date)
            );

            CREATE INDEX IF NOT EXISTS idx_ohlcv_lookup ON ohlcv(ticker_id, timeframe, date);
            CREATE INDEX IF NOT EXISTS idx_signals_active ON signals(is_active, signal_type, detected_at);
            CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
            CREATE INDEX IF NOT EXISTS idx_accum_dist ON accum_dist(ticker_id, date);
        """)
        conn.commit()

        # ── Migrations ──

        # 1. Deduplicate signals before applying UNIQUE index (Fix IntegrityError)
        conn.execute("""
            DELETE FROM signals
            WHERE id NOT IN (
                SELECT MAX(id)
                FROM signals
                GROUP BY symbol, timeframe, signal_type, detected_at
            )
        """)
        conn.commit()

        # 2. Create UNIQUE index for signals upsert
        try:
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_signals_upsert 
                ON signals(symbol, timeframe, signal_type, detected_at)
            """)
            conn.commit()
        except sqlite3.IntegrityError as e:
            logger.error(f"Could not create unique index on signals: {e}")

        # Run migrations for existing databases
        try:
            conn.execute("ALTER TABLE tickers ADD COLUMN volume INTEGER DEFAULT 0")
        except sqlite3.OperationalError: pass # Already exists

        try:
            conn.execute("ALTER TABLE tickers ADD COLUMN avg_volume INTEGER DEFAULT 0")
        except sqlite3.OperationalError: pass # Already exists

        try:
            conn.execute("ALTER TABLE tickers ADD COLUMN volume_updated_at TEXT")
        except sqlite3.OperationalError: pass # Already exists

        conn.commit()
        logger.info(f"Database initialized at {self.db_path}")

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    # ── Tickers ──────────────────────────────────────────────────────────

    def get_or_create_ticker(self, symbol: str, name: str = "", sector: str = "") -> int:
        """Get ticker_id or create new ticker, return id."""
        conn = self._connect()
        row = conn.execute("SELECT id FROM tickers WHERE symbol = ?", (symbol,)).fetchone()
        if row:
            return row["id"]
        cursor = conn.execute(
            "INSERT INTO tickers (symbol, name, sector, updated_at) VALUES (?, ?, ?, ?)",
            (symbol, name, sector, datetime.now().isoformat()),
        )
        conn.commit()
        return cursor.lastrowid

    def get_all_tickers(self) -> list[dict]:
        conn = self._connect()
        rows = conn.execute("SELECT * FROM tickers WHERE is_active = 1 ORDER BY symbol").fetchall()
        return [dict(r) for r in rows]

    def update_ticker_volume(self, symbol: str, volume: int, avg_volume: int):
        """Update ticker volume and avg_volume with timestamp."""
        conn = self._connect()
        conn.execute(
            "UPDATE tickers SET volume = ?, avg_volume = ?, volume_updated_at = ? WHERE symbol = ?",
            (volume, avg_volume, datetime.now().isoformat(), symbol)
        )
        conn.commit()

    def get_next_tickers_for_volume_update(self, limit: int = 50) -> list[str]:
        """Get tickers that haven't been updated for the longest time."""
        conn = self._connect()
        rows = conn.execute("""
            SELECT symbol FROM tickers
            WHERE is_active = 1
            ORDER BY volume_updated_at ASC NULLS FIRST
            LIMIT ?
        """, (limit,)).fetchall()
        return [r["symbol"] for r in rows]

    # ── OHLCV ────────────────────────────────────────────────────────────

    def upsert_ohlcv(self, ticker_id: int, timeframe: str, df: pd.DataFrame):
        """Insert or update OHLCV data from DataFrame."""
        conn = self._connect()
        rows = []
        for _, row in df.iterrows():
            date_str = str(row["date"]) if "date" in row else str(row.name)
            rows.append((
                ticker_id, timeframe, date_str,
                float(row["open"]), float(row["high"]),
                float(row["low"]), float(row["close"]),
                int(row["volume"]) if pd.notna(row["volume"]) else 0,
            ))
        conn.executemany("""
            INSERT OR REPLACE INTO ohlcv (ticker_id, timeframe, date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()

    def get_ohlcv(self, symbol: str, timeframe: str, limit: int = 500) -> pd.DataFrame:
        """Fetch OHLCV data for a symbol + timeframe."""
        conn = self._connect()
        query = """
            SELECT o.date, o.open, o.high, o.low, o.close, o.volume
            FROM ohlcv o
            JOIN tickers t ON t.id = o.ticker_id
            WHERE t.symbol = ? AND o.timeframe = ?
            ORDER BY o.date DESC
            LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(symbol, timeframe, limit))
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
        return df

    # ── Indicators ───────────────────────────────────────────────────────

    def upsert_indicators(self, ticker_id: int, timeframe: str, df: pd.DataFrame):
        """Store calculated indicator values."""
        conn = self._connect()
        rows = []
        for idx, row in df.iterrows():
            date_str = str(idx)
            rows.append((
                ticker_id, timeframe, date_str,
                _safe_float(row, "rsi_14"),
                _safe_float(row, "MACD_12_26_9"),
                _safe_float(row, "MACDs_12_26_9"),
                _safe_float(row, "MACDh_12_26_9"),
                _safe_float(row, "STOCHk_14_3_3"),
                _safe_float(row, "STOCHd_14_3_3"),
                _safe_float(row, "obv"),
                _safe_float(row, "mfi"),
                _safe_float(row, "adl"),
            ))
        conn.executemany("""
            INSERT OR REPLACE INTO indicators
            (ticker_id, timeframe, date, rsi_14, macd, macd_signal, macd_hist,
             stoch_k, stoch_d, obv, mfi, adl)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()

    def get_indicators(self, symbol: str, timeframe: str, limit: int = 500) -> pd.DataFrame:
        """Fetch indicator data for charting."""
        conn = self._connect()
        query = """
            SELECT i.date, i.rsi_14, i.macd, i.macd_signal, i.macd_hist,
                   i.stoch_k, i.stoch_d, i.obv, i.mfi, i.adl
            FROM indicators i
            JOIN tickers t ON t.id = i.ticker_id
            WHERE t.symbol = ? AND i.timeframe = ?
            ORDER BY i.date DESC
            LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(symbol, timeframe, limit))
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
        return df

    # ── Signals ──────────────────────────────────────────────────────────

    def store_signal(self, ticker_id: int, symbol: str, signal: dict):
        """Store a single detected signal."""
        conn = self._connect()
        
        # 1. Start with existing metadata if present
        metadata = signal.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        else:
            metadata = metadata.copy() # Avoid mutating original
            
        # 2. Add other non-core fields to metadata (e.g., indicator, divergence_strength)
        core_fields = ("type", "ticker", "timeframe", "date", "confidence_score", "bar_index", "metadata")
        for k, v in signal.items():
            if k not in core_fields:
                metadata[k] = v

        # 3. Convert non-serializable types (datetime, numpy items)
        clean_meta = {}
        for k, v in metadata.items():
            if hasattr(v, "isoformat"):
                clean_meta[k] = v.isoformat()
            elif hasattr(v, "item"):
                clean_meta[k] = v.item()
            else:
                clean_meta[k] = v

        date_str = str(signal.get("date", datetime.now().isoformat()))
        conn.execute("""
            INSERT OR REPLACE INTO signals
            (ticker_id, symbol, timeframe, signal_type, detected_at, confidence_score, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            ticker_id,
            symbol,
            signal.get("timeframe", ""),
            signal.get("type", ""),
            date_str,
            signal.get("confidence_score", 0),
            json.dumps(clean_meta),
        ))
        conn.commit()

    def deactivate_old_signals(self, symbol: str, days_old: int = 30):
        """Deactivate signals older than N days."""
        conn = self._connect()
        conn.execute("""
            UPDATE signals SET is_active = 0
            WHERE symbol = ?
            AND julianday('now') - julianday(detected_at) > ?
        """, (symbol, days_old))
        conn.commit()

    def get_active_signals(
        self,
        signal_type: str | None = None,
        timeframe: str | None = None,
        min_confidence: float = 0.0,
        limit: int = 200,
        offset: int = 0,
        max_age_days: int = 7,
    ) -> list[dict]:
        """
        Fetch active screening results with optional filters.

        PERBAIKAN BUG: Versi lama tidak memanggil self._connect() sama sekali,
        sehingga variabel 'conn' tidak terdefinisi dan setiap request ke
        endpoint /api/screener/results akan crash dengan NameError.
        """
        # FIXED: conn sekarang didefinisikan dengan benar sebelum dipakai
        conn = self._connect()

        # Signal Age Filter: Only show 7 latest days of signals
        max_age_days = 7

        query = """
            SELECT s.*, t.name as ticker_name, t.sector,
                   (SELECT o.close FROM ohlcv o
                    WHERE o.ticker_id = s.ticker_id AND o.timeframe = s.timeframe
                    ORDER BY o.date DESC LIMIT 1) as latest_price
            FROM signals s
            JOIN tickers t ON t.id = s.ticker_id
            WHERE s.is_active = 1
            AND julianday('now') - julianday(detected_at) <= ?
            AND s.confidence_score >= ?
            AND t.volume >= ?
            AND t.avg_volume >= ?
        """
        params: list = [max_age_days, min_confidence, MIN_LIQUIDITY_VOLUME, MIN_LIQUIDITY_AVG_VOLUME]

        if signal_type:
            query += " AND s.signal_type = ?"
            params.append(signal_type)
        if timeframe:
            query += " AND s.timeframe = ?"
            params.append(timeframe)

        query += " ORDER BY s.confidence_score DESC, s.detected_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = conn.execute(query, params).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            if d.get("metadata"):
                try:
                    d["metadata"] = json.loads(d["metadata"])
                except (json.JSONDecodeError, TypeError):
                    d["metadata"] = {}
            results.append(d)
        return results

    def get_signal_summary(self, min_confidence: float = 0.5) -> dict:
        """
        Get summary stats of active signals, filtered by min_confidence.

        UPDATED: menerima parameter min_confidence agar summary
        konsisten dengan filter yang dipakai di screener results.
        """
        conn = self._connect()
        total = conn.execute("""
            SELECT COUNT(*) FROM signals s
            JOIN tickers t ON t.id = s.ticker_id
            WHERE s.is_active = 1 AND s.confidence_score >= ?
            AND t.volume >= ? AND t.avg_volume >= ?
        """, (min_confidence, MIN_LIQUIDITY_VOLUME, MIN_LIQUIDITY_AVG_VOLUME)).fetchone()[0]

        by_type = conn.execute("""
            SELECT s.signal_type, COUNT(*) as cnt
            FROM signals s
            JOIN tickers t ON t.id = s.ticker_id
            WHERE s.is_active = 1 AND s.confidence_score >= ?
            AND t.volume >= ? AND t.avg_volume >= ?
            GROUP BY s.signal_type
            ORDER BY cnt DESC
        """, (min_confidence, MIN_LIQUIDITY_VOLUME, MIN_LIQUIDITY_AVG_VOLUME)).fetchall()

        top_stocks = conn.execute("""
            SELECT s.symbol, MAX(s.confidence_score) as max_score,
                   COUNT(*) as signal_count,
                   GROUP_CONCAT(DISTINCT s.signal_type) as signal_types
            FROM signals s
            JOIN tickers t ON t.id = s.ticker_id
            WHERE s.is_active = 1 AND s.confidence_score >= ?
            AND t.volume >= ? AND t.avg_volume >= ?
            GROUP BY s.symbol
            ORDER BY max_score DESC
            LIMIT 10
        """, (min_confidence, MIN_LIQUIDITY_VOLUME, MIN_LIQUIDITY_AVG_VOLUME)).fetchall()

        return {
            "total_signals": total,
            "min_confidence_filter": min_confidence,
            "by_type": {r["signal_type"]: r["cnt"] for r in by_type},
            "top_stocks": [dict(r) for r in top_stocks],
            "server_time": datetime.now().isoformat()
        }

    def get_signals_for_ticker(self, symbol: str) -> list[dict]:
        """Get all active signals for a specific ticker."""
        conn = self._connect()
        rows = conn.execute("""
            SELECT * FROM signals
            WHERE symbol = ? AND is_active = 1
            ORDER BY confidence_score DESC
        """, (symbol,)).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            if d.get("metadata"):
                try:
                    d["metadata"] = json.loads(d["metadata"])
                except (json.JSONDecodeError, TypeError):
                    d["metadata"] = {}
            results.append(d)
        return results

    # ── Accumulation/Distribution ─────────────────────────────────────────

    def upsert_accum_dist(self, ticker_id: int, accum_result: dict, df: pd.DataFrame):
        """Store accumulation/distribution analysis result."""
        if not accum_result or df.empty:
            return
        conn = self._connect()
        date_str = str(df.index[-1])
        conn.execute("""
            INSERT OR REPLACE INTO accum_dist
            (ticker_id, date, phase, adl_value, obv_value, mfi_value, volume_ratio)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            ticker_id,
            date_str,
            accum_result.get("phase", "neutral"),
            accum_result.get("adl_latest"),
            accum_result.get("obv_latest"),
            accum_result.get("mfi_latest"),
            accum_result.get("volume_ratio"),
        ))
        conn.commit()

    def get_accum_dist(self, symbol: str, limit: int = 60) -> list[dict]:
        """Get accumulation/distribution history for a ticker."""
        conn = self._connect()
        rows = conn.execute("""
            SELECT a.* FROM accum_dist a
            JOIN tickers t ON t.id = a.ticker_id
            WHERE t.symbol = ?
            ORDER BY a.date DESC LIMIT ?
        """, (symbol, limit)).fetchall()
        return [dict(r) for r in rows]

    # ── Utility ──────────────────────────────────────────────────────────

    def clear_signals(self):
        """Clear all signals (for fresh scan)."""
        conn = self._connect()
        conn.execute("DELETE FROM signals")
        conn.commit()

    def get_last_ohlcv_date(self, symbol: str, timeframe: str) -> str | None:
        """Get the most recent OHLCV date for incremental updates."""
        conn = self._connect()
        row = conn.execute("""
            SELECT MAX(o.date) as last_date
            FROM ohlcv o JOIN tickers t ON t.id = o.ticker_id
            WHERE t.symbol = ? AND o.timeframe = ?
        """, (symbol, timeframe)).fetchone()
        return row["last_date"] if row else None


def _safe_float(row, col: str) -> float | None:
    """Safely extract float value from DataFrame row."""
    if col in row.index:
        val = row[col]
        if pd.notna(val):
            return float(val)
    return None