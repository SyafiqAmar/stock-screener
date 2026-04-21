"""
Asynchronous PostgreSQL database manager using SQLAlchemy 2.0.
Supports high-concurrency connections and modern async/await patterns.
"""
import json
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

import pandas as pd
import numpy as np
from sqlalchemy import select, update, delete, insert, func, text, desc, and_
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from backend.config import DATABASE_URL, MIN_LIQUIDITY_VOLUME, MIN_LIQUIDITY_AVG_VOLUME
from backend.storage.models import Base, Ticker, OHLCV, Indicator, Signal, AccumDist

logger = logging.getLogger(__name__)

class StockDatabase:
    """Asynchronous database interface for PostgreSQL/SQLAlchemy."""

    def __init__(self, database_url: str | None = None):
        self.url = database_url or DATABASE_URL
        # create_async_engine is required for asyncpg
        self.engine = create_async_engine(
            self.url,
            echo=False,
            pool_size=20,
            max_overflow=10,
        )
        self.async_session = async_sessionmaker(
            self.engine, expire_on_commit=False, class_=AsyncSession
        )

    async def initialize(self, retries: int = 5, delay: int = 5):
        """Create tables if they don't exist with retry logic for Docker synchronization."""
        last_error = None
        for i in range(retries):
            try:
                async with self.engine.begin() as conn:
                    # Test connection with a simple query
                    await conn.execute(text("SELECT 1"))
                    # Create all tables defined in models.py
                    await conn.run_sync(Base.metadata.create_all)
                logger.info("✅ Database initialized successfully (Async mode)")
                return
            except Exception as e:
                last_error = e
                logger.warning(f"⏳ Database not ready (attempt {i+1}/{retries}): {e}")
                await asyncio.sleep(delay)
        
        logger.error(f"❌ Database initialization failed after {retries} attempts: {last_error}")
        raise last_error

    async def close(self):
        """Dispose of the engine."""
        await self.engine.dispose()

    # ── Tickers ──────────────────────────────────────────────────────────

    async def get_or_create_ticker(self, symbol: str, name: str = "", sector: str = "") -> int:
        async with self.async_session() as session:
            stmt = select(Ticker.id).where(Ticker.symbol == symbol)
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()
            
            if row:
                return row
            
            new_ticker = Ticker(
                symbol=symbol, 
                name=name, 
                sector=sector, 
                updated_at=datetime.utcnow()
            )
            session.add(new_ticker)
            await session.commit()
            return new_ticker.id

    async def get_all_tickers(self) -> List[Dict[str, Any]]:
        async with self.async_session() as session:
            stmt = select(Ticker).where(Ticker.is_active == True).order_by(Ticker.symbol)
            result = await session.execute(stmt)
            return [self._to_dict(t) for t in result.scalars().all()]

    async def update_ticker_volume(self, symbol: str, volume: int, avg_volume: int):
        async with self.async_session() as session:
            stmt = (
                update(Ticker)
                .where(Ticker.symbol == symbol)
                .values(
                    volume=volume,
                    avg_volume=avg_volume,
                    volume_updated_at=datetime.utcnow()
                )
            )
            await session.execute(stmt)
            await session.commit()

    async def get_next_tickers_for_volume_update(self, limit: int = 50) -> List[str]:
        """Get tickers that haven't been updated for the longest time (Async)."""
        async with self.async_session() as session:
            stmt = (
                select(Ticker.symbol)
                .where(Ticker.is_active == True)
                .order_by(Ticker.volume_updated_at.asc().nullsfirst())
                .limit(limit)
            )
            result = await session.execute(stmt)
            return [str(r) for r in result.scalars().all()]

    # ── OHLCV ────────────────────────────────────────────────────────────

    async def upsert_ohlcv(self, ticker_id: int, timeframe: str, df: pd.DataFrame):
        if df.empty:
            return
            
        async with self.async_session() as session:
            records = []
            for _, row in df.iterrows():
                date_val = row["date"] if "date" in row else row.name
                if hasattr(date_val, "tzinfo") and date_val.tzinfo is not None:
                    date_val = date_val.replace(tzinfo=None)
                elif isinstance(date_val, str):
                    date_val = pd.to_datetime(date_val).to_pydatetime().replace(tzinfo=None)
                
                records.append({
                    "ticker_id": ticker_id,
                    "timeframe": timeframe,
                    "date": date_val,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row["volume"]) if pd.notna(row["volume"]) else 0,
                })
            
            # Using PostgreSQL-specific ON CONFLICT via text or specialized dialect features
            # For simplicity across async drivers, we'll use a manual upsert pattern or execute_many
            # Postgres supports 'INSERT INTO ... ON CONFLICT ...'
            for record in records:
                # In a real high-throughput app, we'd use dialet-specific upserts
                # but for simplicity let's use the provided logic
                stmt = text("""
                    INSERT INTO ohlcv (ticker_id, timeframe, date, open, high, low, close, volume)
                    VALUES (:ticker_id, :timeframe, :date, :open, :high, :low, :close, :volume)
                    ON CONFLICT(ticker_id, timeframe, date) DO UPDATE SET
                    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                    close=EXCLUDED.close, volume=EXCLUDED.volume
                """)
                await session.execute(stmt, record)
            
            await session.commit()

    async def get_ohlcv(self, symbol: str, timeframe: str, limit: int = 500) -> pd.DataFrame:
        async with self.async_session() as session:
            stmt = (
                select(OHLCV.date, OHLCV.open, OHLCV.high, OHLCV.low, OHLCV.close, OHLCV.volume)
                .join(Ticker)
                .where(and_(Ticker.symbol == symbol, OHLCV.timeframe == timeframe))
                .order_by(desc(OHLCV.date))
                .limit(limit)
            )
            result = await session.execute(stmt)
            rows = result.all()
            
            if not rows:
                return pd.DataFrame()
            
            df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
            df["date"] = pd.to_datetime(df["date"])
            return df.sort_values("date").reset_index(drop=True)

    # ── Indicators ───────────────────────────────────────────────────────

    async def upsert_indicators(self, ticker_id: int, timeframe: str, df: pd.DataFrame):
        if df.empty:
            return
            
        async with self.async_session() as session:
            for idx, row in df.iterrows():
                date_val = idx if df.index.name == 'date' else row.get('date')
                if hasattr(date_val, "tzinfo") and date_val.tzinfo is not None:
                    date_val = date_val.replace(tzinfo=None)
                
                record = {
                    "ticker_id": ticker_id,
                    "timeframe": timeframe,
                    "date": date_val,
                    "rsi_14": _s_f(row, "rsi_14"),
                    "macd": _s_f(row, "MACD_12_26_9"),
                    "macd_signal": _s_f(row, "MACDs_12_26_9"),
                    "macd_hist": _s_f(row, "MACDh_12_26_9"),
                    "stoch_k": _s_f(row, "STOCHk_14_3_3"),
                    "stoch_d": _s_f(row, "STOCHd_14_3_3"),
                    "obv": _s_f(row, "obv"),
                    "mfi": _s_f(row, "mfi"),
                    "adl": _s_f(row, "adl"),
                }
                
                stmt = text("""
                    INSERT INTO indicators (ticker_id, timeframe, date, rsi_14, macd, macd_signal, macd_hist, stoch_k, stoch_d, obv, mfi, adl)
                    VALUES (:ticker_id, :timeframe, :date, :rsi_14, :macd, :macd_signal, :macd_hist, :stoch_k, :stoch_d, :obv, :mfi, :adl)
                    ON CONFLICT(ticker_id, timeframe, date) DO UPDATE SET
                    rsi_14=EXCLUDED.rsi_14, macd=EXCLUDED.macd, macd_signal=EXCLUDED.macd_signal,
                    macd_hist=EXCLUDED.macd_hist, stoch_k=EXCLUDED.stoch_k, stoch_d=EXCLUDED.stoch_d,
                    obv=EXCLUDED.obv, mfi=EXCLUDED.mfi, adl=EXCLUDED.adl
                """)
                await session.execute(stmt, record)
            
            await session.commit()

    async def get_indicators(self, symbol: str, timeframe: str, limit: int = 500) -> pd.DataFrame:
        """Fetch indicator data for charting (Async)."""
        async with self.async_session() as session:
            stmt = (
                select(
                    Indicator.date, Indicator.rsi_14, Indicator.macd, 
                    Indicator.macd_signal, Indicator.macd_hist,
                    Indicator.stoch_k, Indicator.stoch_d, Indicator.obv, 
                    Indicator.mfi, Indicator.adl
                )
                .join(Ticker)
                .where(and_(Ticker.symbol == symbol, Indicator.timeframe == timeframe))
                .order_by(desc(Indicator.date))
                .limit(limit)
            )
            result = await session.execute(stmt)
            rows = result.all()
            
            if not rows:
                return pd.DataFrame()
            
            df = pd.DataFrame(rows, columns=[
                "date", "rsi_14", "macd", "macd_signal", "macd_hist",
                "stoch_k", "stoch_d", "obv", "mfi", "adl"
            ])
            df["date"] = pd.to_datetime(df["date"])
            return df.sort_values("date").reset_index(drop=True)

    # ── Signals ──────────────────────────────────────────────────────────

    async def store_signal(self, ticker_id: int, symbol: str, signal: dict):
        async with self.async_session() as session:
            additional_data = signal.get("metadata", {}) or signal.get("additional_data", {})
            if not isinstance(additional_data, dict): additional_data = {}
            
            trade_setup = additional_data.get("trade_setup", {})
            
            # Clean non-serializable
            clean_meta = {}
            for k, v in additional_data.items():
                if hasattr(v, "isoformat"): clean_meta[k] = v.isoformat()
                elif hasattr(v, "item"): clean_meta[k] = v.item()
                else: clean_meta[k] = v

            date_val = signal.get("date", datetime.utcnow())
            if isinstance(date_val, str): date_val = pd.to_datetime(date_val)

            # PostgreSQL Upsert
            stmt = text("""
                INSERT INTO signals 
                (ticker_id, symbol, timeframe, signal_type, detected_at, confidence_score, additional_data, entry, stop_loss, target_1, target_2, is_active)
                VALUES (:ticker_id, :symbol, :timeframe, :signal_type, :detected_at, :confidence_score, :additional_data, :entry, :stop_loss, :target_1, :target_2, TRUE)
                ON CONFLICT(symbol, timeframe, signal_type, detected_at) DO UPDATE SET
                confidence_score=EXCLUDED.confidence_score, additional_data=EXCLUDED.additional_data,
                entry=EXCLUDED.entry, stop_loss=EXCLUDED.stop_loss, target_1=EXCLUDED.target_1, target_2=EXCLUDED.target_2,
                is_active=TRUE
            """)
            
            params = {
                "ticker_id": ticker_id,
                "symbol": symbol,
                "timeframe": signal.get("timeframe"),
                "signal_type": signal.get("type"),
                "detected_at": date_val,
                "confidence_score": float(signal.get("confidence_score", 0)),
                "additional_data": json.dumps(clean_meta),
                "entry": trade_setup.get("entry"),
                "stop_loss": trade_setup.get("stop_loss"),
                "target_1": trade_setup.get("target_1"),
                "target_2": trade_setup.get("target_2")
            }
            
            await session.execute(stmt, params)
            await session.commit()

    async def deactivate_old_signals(self, symbol: str, days_old: int = 30):
        """Set is_active=False for signals older than X days for a specific ticker."""
        async with self.async_session() as session:
            cutoff = datetime.utcnow() - timedelta(days=days_old)
            stmt = (
                update(Signal)
                .where(and_(
                    Signal.symbol == symbol,
                    Signal.detected_at < cutoff,
                    Signal.is_active == True
                ))
                .values(is_active=False)
            )
            await session.execute(stmt)
            await session.commit()

    async def get_active_signals(
        self,
        signal_type: str | None = None,
        signal_types: list[str] | None = None,
        timeframe: str | None = None,
        min_confidence: float = 0.0,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        async with self.async_session() as session:
            # Base query
            query_str = """
                SELECT s.*, t.name as ticker_name, t.sector,
                       (SELECT o.close FROM ohlcv o 
                        WHERE o.ticker_id = s.ticker_id AND o.timeframe = s.timeframe 
                        ORDER BY o.date DESC LIMIT 1) as current_price
                FROM signals s
                JOIN tickers t ON t.id = s.ticker_id
                WHERE s.is_active = TRUE
                AND s.detected_at > (CURRENT_TIMESTAMP - INTERVAL '7 days')
                AND s.confidence_score >= :min_conf
                AND t.volume >= :min_vol
                AND t.avg_volume >= :min_avg_vol
            """
            
            params = {
                "min_conf": min_confidence,
                "min_vol": MIN_LIQUIDITY_VOLUME,
                "min_avg_vol": MIN_LIQUIDITY_AVG_VOLUME
            }

            if signal_type:
                query_str += " AND s.signal_type = :sig_type"
                params["sig_type"] = signal_type
            elif signal_types:
                query_str += " AND s.signal_type = ANY(:sig_types)"
                params["sig_types"] = list(signal_types)

            if timeframe:
                query_str += " AND s.timeframe = :tf"
                params["tf"] = timeframe

            query_str += " ORDER BY s.confidence_score DESC, s.detected_at DESC LIMIT :limit OFFSET :offset"
            params["limit"] = limit
            params["offset"] = offset

            query = text(query_str)
            result = await session.execute(query, params)
            rows = result.mappings().all()
            
            final = []
            for r in rows:
                d = dict(r)
                if isinstance(d.get("additional_data"), str):
                    try: d["additional_data"] = json.loads(d["additional_data"])
                    except: d["additional_data"] = {}
                final.append(d)
            return final

    async def get_signal_summary(self, min_confidence: float = 0.5) -> Dict[str, Any]:
        async with self.async_session() as session:
            # Total count
            stmt_total = select(func.count(Signal.id)).join(Ticker).where(and_(
                Signal.is_active == True,
                Signal.confidence_score >= min_confidence,
                Ticker.volume >= MIN_LIQUIDITY_VOLUME
            ))
            total = await session.execute(stmt_total)
            
            # By type
            stmt_types = (
                select(Signal.signal_type, func.count(Signal.id).label("cnt"))
                .join(Ticker)
                .where(and_(Signal.is_active == True, Signal.confidence_score >= min_confidence, Ticker.volume >= MIN_LIQUIDITY_VOLUME))
                .group_by(Signal.signal_type)
            )
            types_res = await session.execute(stmt_types)
            
            return {
                "total_signals": total.scalar(),
                "by_type": {r.signal_type: r.cnt for r in types_res.all()},
                "server_time": datetime.utcnow().isoformat()
            }

    # ── Utilities ────────────────────────────────────────────────────────

            await session.commit()

    # ── Utilities ────────────────────────────────────────────────────────
    async def get_last_ohlcv_date(self, symbol: str, timeframe: str) -> Optional[datetime]:
        """Get the most recent OHLCV date for incremental updates (Async)."""
        async with self.async_session() as session:
            stmt = (
                select(func.max(OHLCV.date))
                .join(Ticker)
                .where(and_(Ticker.symbol == symbol, OHLCV.timeframe == timeframe))
            )
            result = await session.execute(stmt)
            return result.scalar()

    async def upsert_accum_dist(self, ticker_id: int, result: dict, df: pd.DataFrame):
        if not result or df.empty: return
        async with self.async_session() as session:
            last_date = pd.to_datetime(df.index[-1])
            stmt = text("""
                INSERT INTO accum_dist (ticker_id, date, phase, adl_value, obv_value, mfi_value, volume_ratio)
                VALUES (:tid, :date, :phase, :adl, :obv, :mfi, :v_ratio)
                ON CONFLICT(ticker_id, date) DO UPDATE SET
                phase=EXCLUDED.phase, adl_value=EXCLUDED.adl_value, obv_value=EXCLUDED.obv_value
            """)
            await session.execute(stmt, {
                "tid": ticker_id, "date": last_date, "phase": result.get("phase"),
                "adl": result.get("adl_latest"), "obv": result.get("obv_latest"),
                "mfi": result.get("mfi_latest"), "v_ratio": result.get("volume_ratio")
            })
            await session.commit()

    async def get_signals_for_ticker(self, symbol: str) -> List[Dict[str, Any]]:
        """Get all active signals for a specific ticker (Async)."""
        async with self.async_session() as session:
            stmt = (
                select(Signal)
                .join(Ticker)
                .where(and_(Ticker.symbol == symbol, Signal.is_active == True))
                .order_by(desc(Signal.detected_at))
            )
            result = await session.execute(stmt)
            return [self._to_dict(s) for s in result.scalars().all()]

    async def get_accum_dist(self, symbol: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get accumulation/distribution history for a specific ticker (Async)."""
        async with self.async_session() as session:
            # Join Ticker to filter by symbol
            stmt = (
                select(AccumDist)
                .join(Ticker)
                .where(Ticker.symbol == symbol)
                .order_by(desc(AccumDist.date))
                .limit(limit)
            )
            result = await session.execute(stmt)
            return [self._to_dict(a) for a in result.scalars().all()]

    def _to_dict(self, obj):
        return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}

def _s_f(row, col: str) -> float | None:
    if col in row.index:
        val = row[col]
        return float(val) if pd.notna(val) else None
    return None