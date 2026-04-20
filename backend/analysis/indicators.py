"""
Technical indicator calculations using pure pandas.
(Manual implementation to avoid pandas_ta/numba compatibility issues on Python 3.14)
Computes RSI, MACD, Stochastic, OBV, MFI, and ADL.
"""
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def calculate_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate all technical indicators on an OHLCV DataFrame.
    Adds technical columns required for the analysis engine.
    """
    if df.empty or len(df) < 26:
        logger.warning("DataFrame too short for indicator calculation")
        return df

    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    # 1. RSI (14)
    df["rsi_14"] = compute_rsi(df["close"], length=14)

    # 2. MACD (12, 26, 9)
    macd_vals, macd_signal, macd_hist = compute_macd(df["close"], fast=12, slow=26, signal=9)
    df["MACD_12_26_9"] = macd_vals
    df["MACDs_12_26_9"] = macd_signal
    df["MACDh_12_26_9"] = macd_hist
    df["macd_hist"] = macd_hist

    # 3. Stochastic (14, 3, 3)
    stoch_k, stoch_d = compute_stochastic(df["high"], df["low"], df["close"], k=14, d=3)
    df["STOCHk_14_3_3"] = stoch_k
    df["STOCHd_14_3_3"] = stoch_d
    df["stoch_k"] = stoch_k

    # 4. OBV
    df["obv"] = compute_obv(df["close"], df["volume"])

    # 5. MFI (14) — FIXED: sekarang menerima series saja, tidak bergantung pada 'df'
    df["mfi"] = compute_mfi(df["high"], df["low"], df["close"], df["volume"], length=14)

    # 6. ADL (Accumulation/Distribution Line)
    df["adl"] = compute_adl(df["high"], df["low"], df["close"], df["volume"])

    return df


def compute_rsi(series: pd.Series, length: int = 14) -> pd.Series:
    """
    Wilder's RSI menggunakan EWM (Exponential Weighted Mean).

    PERBAIKAN: Versi lama memakai rolling().mean() lalu loop manual yang
    memodifikasi hasil rolling — logika yang salah dan O(n²).
    EWM dengan alpha=1/length adalah implementasi Wilder yang benar dan O(n).
    """
    delta = series.diff()

    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    # Wilder smoothing = EWM dengan alpha = 1/length, adjust=False
    avg_gain = gain.ewm(alpha=1 / length, min_periods=length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, min_periods=length, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


def compute_macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    exp1 = series.ewm(span=fast, adjust=False).mean()
    exp2 = series.ewm(span=slow, adjust=False).mean()
    macd = exp1 - exp2
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - signal_line
    return macd, signal_line, hist


def compute_stochastic(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    k: int = 14,
    d: int = 3,
):
    low_min = low.rolling(window=k).min()
    high_max = high.rolling(window=k).max()
    denom = high_max - low_min
    # Hindari pembagian dengan nol saat high == low (saham tidak bergerak)
    stoch_k = 100 * (close - low_min) / denom.replace(0, np.nan)
    stoch_d = stoch_k.rolling(window=d).mean()
    return stoch_k, stoch_d


def compute_obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    return (np.sign(close.diff()) * volume).fillna(0).cumsum()


def compute_mfi(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
    length: int = 14,
) -> pd.Series:
    """
    Money Flow Index (MFI).

    PERBAIKAN BUG: Versi lama memakai 'df.index' di dalam fungsi ini,
    padahal 'df' tidak ada dalam scope compute_mfi() — akan langsung
    NameError saat dipanggil.

    Fix: gunakan 'tp.index' (Typical Price) yang memang ada di scope ini,
    karena tp dihitung dari high/low/close yang sudah diteruskan sebagai argumen.

    Juga ditambahkan guard pembagian-nol untuk neg_mf = 0
    (terjadi saat semua bar dalam window adalah up-day).
    """
    # Typical Price
    tp = (high + low + close) / 3

    # Raw Money Flow
    rmf = tp * volume

    # Arah money flow berdasarkan perubahan Typical Price
    tp_diff = tp.diff()

    # FIXED: gunakan tp.index, bukan df.index
    pos_mf = pd.Series(0.0, index=tp.index)
    neg_mf = pd.Series(0.0, index=tp.index)

    pos_mf[tp_diff > 0] = rmf[tp_diff > 0]
    neg_mf[tp_diff < 0] = rmf[tp_diff < 0]

    pos_sum = pos_mf.rolling(window=length).sum()
    neg_sum = neg_mf.rolling(window=length).sum()

    # Guard: hindari pembagian nol (semua bar up → neg_sum = 0 → MFI = 100)
    mfr = pos_sum / neg_sum.replace(0, np.nan)

    mfi = 100 - (100 / (1 + mfr))

    # Saat neg_sum == 0 (semua up), MFI seharusnya 100
    mfi = mfi.fillna(100.0)

    return mfi


def compute_adl(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
) -> pd.Series:
    """
    Accumulation/Distribution Line.
    CLV (Close Location Value) menunjukkan posisi close relatif terhadap range bar.
    """
    hl_range = high - low
    # Hindari pembagian nol saat high == low (doji atau saham suspend)
    clv = ((close - low) - (high - close)) / hl_range.replace(0, np.nan)
    clv = clv.fillna(0)
    return (clv * volume).cumsum()