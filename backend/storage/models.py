from datetime import datetime
from typing import Optional
from sqlalchemy import (
    Column, Integer, BigInteger, String, Float, DateTime, 
    ForeignKey, UniqueConstraint, Index, JSON, Boolean, Text
)
from sqlalchemy.orm import DeclarativeBase, relationship

class Base(DeclarativeBase):
    pass

class Ticker(Base):
    __tablename__ = "tickers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, unique=True, nullable=False, index=True)
    name = Column(String, default="")
    sector = Column(String, default="")
    board = Column(String, default="")
    volume = Column(BigInteger, default=0)
    avg_volume = Column(BigInteger, default=0)
    volume_updated_at = Column(DateTime)
    is_active = Column(Boolean, default=True)
    updated_at = Column(DateTime, default=datetime.utcnow)

    ohlcv = relationship("OHLCV", back_populates="ticker", cascade="all, delete-orphan")
    indicators = relationship("Indicator", back_populates="ticker", cascade="all, delete-orphan")
    signals = relationship("Signal", back_populates="ticker", cascade="all, delete-orphan")
    accum_dist = relationship("AccumDist", back_populates="ticker", cascade="all, delete-orphan")

class OHLCV(Base):
    __tablename__ = "ohlcv"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker_id = Column(Integer, ForeignKey("tickers.id"), nullable=False)
    timeframe = Column(String, nullable=False)
    date = Column(DateTime, nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger, default=0)

    ticker = relationship("Ticker", back_populates="ohlcv")

    __table_args__ = (
        UniqueConstraint("ticker_id", "timeframe", "date", name="uix_ohlcv_lookup"),
        Index("idx_ohlcv_ticker_tf", "ticker_id", "timeframe"),
    )

class Indicator(Base):
    __tablename__ = "indicators"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker_id = Column(Integer, ForeignKey("tickers.id"), nullable=False)
    timeframe = Column(String, nullable=False)
    date = Column(DateTime, nullable=False)
    
    # Standard technical indicators
    rsi_14 = Column(Float)
    macd = Column(Float)
    macd_signal = Column(Float)
    macd_hist = Column(Float)
    stoch_k = Column(Float)
    stoch_d = Column(Float)
    obv = Column(Float)
    mfi = Column(Float)
    adl = Column(Float)

    ticker = relationship("Ticker", back_populates="indicators")

    __table_args__ = (
        UniqueConstraint("ticker_id", "timeframe", "date", name="uix_indicators_lookup"),
    )

class Signal(Base):
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker_id = Column(Integer, ForeignKey("tickers.id"), nullable=False)
    symbol = Column(String, nullable=False, index=True)
    timeframe = Column(String, nullable=False)
    signal_type = Column(String, nullable=False)
    detected_at = Column(DateTime, nullable=False)
    confidence_score = Column(Float, default=0)
    additional_data = Column(JSON, default={})
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Flattened fields for fast UI access
    entry = Column(Float)
    stop_loss = Column(Float)
    target_1 = Column(Float)
    target_2 = Column(Float)

    ticker = relationship("Ticker", back_populates="signals")

    __table_args__ = (
        UniqueConstraint("symbol", "timeframe", "signal_type", "detected_at", name="uix_signals_upsert"),
        Index("idx_active_signals", "is_active", "signal_type", "detected_at"),
    )

class AccumDist(Base):
    __tablename__ = "accum_dist"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker_id = Column(Integer, ForeignKey("tickers.id"), nullable=False)
    date = Column(DateTime, nullable=False)
    phase = Column(String) # accumulation, distribution, neutral
    adl_value = Column(Float)
    obv_value = Column(Float)
    mfi_value = Column(Float)
    volume_ratio = Column(Float)

    ticker = relationship("Ticker", back_populates="accum_dist")

    __table_args__ = (
        UniqueConstraint("ticker_id", "date", name="uix_accum_dist_lookup"),
    )
