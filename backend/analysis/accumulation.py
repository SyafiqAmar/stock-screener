"""
Accumulation & Distribution phase analyzer.
Determines whether smart money is accumulating or distributing based on
ADL, OBV, MFI, and volume analysis.
"""
import logging
import pandas as pd
import numpy as np

from backend.config import ACCUM_TREND_PERIOD, ACCUM_VOLUME_AVG_PERIOD

logger = logging.getLogger(__name__)


def analyze_accumulation_distribution(df: pd.DataFrame) -> dict | None:
    """
    Analyze the current Accumulation/Distribution phase.

    Uses 4 signals:
        1. ADL (Accumulation/Distribution Line) trend
        2. OBV (On-Balance Volume) trend
        3. MFI (Money Flow Index) level
        4. Volume ratio vs moving average

    Phase classification:
        - 'accumulation': smart money buying (ADL↑, OBV↑, volume increasing)
        - 'distribution': smart money selling (ADL↓, OBV↓)
        - 'neutral': mixed signals

    Args:
        df: DataFrame with adl, obv, mfi, volume columns (DatetimeIndex).

    Returns:
        Dict with phase analysis results, or None if insufficient data.
    """
    if df.empty or len(df) < ACCUM_TREND_PERIOD + 5:
        return None

    result = {}

    # 1. ADL trend
    if "adl" in df.columns and df["adl"].notna().sum() > ACCUM_TREND_PERIOD:
        result["adl_trend"] = _classify_trend(df["adl"], ACCUM_TREND_PERIOD)
        result["adl_latest"] = float(df["adl"].dropna().iloc[-1])
    else:
        result["adl_trend"] = "unknown"
        result["adl_latest"] = None

    # 2. OBV trend
    if "obv" in df.columns and df["obv"].notna().sum() > ACCUM_TREND_PERIOD:
        result["obv_trend"] = _classify_trend(df["obv"], ACCUM_TREND_PERIOD)
        result["obv_latest"] = float(df["obv"].dropna().iloc[-1])
    else:
        result["obv_trend"] = "unknown"
        result["obv_latest"] = None

    # 3. MFI level
    if "mfi" in df.columns and df["mfi"].notna().any():
        mfi_val = float(df["mfi"].dropna().iloc[-1])
        result["mfi_level"] = _categorize_mfi(mfi_val)
        result["mfi_latest"] = mfi_val
    else:
        result["mfi_level"] = "unknown"
        result["mfi_latest"] = None

    # 4. Volume ratio
    if "volume" in df.columns and df["volume"].notna().sum() > ACCUM_VOLUME_AVG_PERIOD:
        vol_avg = df["volume"].rolling(ACCUM_VOLUME_AVG_PERIOD).mean().iloc[-1]
        latest_vol = df["volume"].iloc[-1]
        result["volume_ratio"] = round(float(latest_vol / vol_avg), 3) if vol_avg > 0 else 0
        result["volume_avg"] = float(vol_avg)
        result["volume_latest"] = float(latest_vol)
    else:
        result["volume_ratio"] = None
        result["volume_avg"] = None
        result["volume_latest"] = None

    # 5. Price-Volume divergence (price up but volume declining = distribution warning)
    if "close" in df.columns:
        price_trend = _classify_trend(df["close"], ACCUM_TREND_PERIOD)
        result["price_trend"] = price_trend
    else:
        result["price_trend"] = "unknown"

    # ── Determine overall phase ──
    result["phase"] = _determine_phase(result)
    result["phase_confidence"] = _phase_confidence(result)

    # 6. Advanced Metrics (Accumulation Value and Avg Price)
    # Estimate Money Flow over the trend period
    recent_df = df.tail(ACCUM_TREND_PERIOD).copy()
    if not recent_df.empty and "close" in recent_df.columns and "volume" in recent_df.columns:
        # Typical VWAP logic for avg price
        total_vol = recent_df["volume"].sum()
        if total_vol > 0:
            result["avg_price_accum"] = float((recent_df["close"] * recent_df["volume"]).sum() / total_vol)
        else:
            result["avg_price_accum"] = float(recent_df["close"].iloc[-1])

        # Heuristic for Money Flow: (2*Close - High - Low) / (High - Low) * Volume * Price
        # This approximates intraday accumulation/distribution when broker data is missing
        if "high" in recent_df.columns and "low" in recent_df.columns:
            # Avoid division by zero
            denom = (recent_df["high"] - recent_df["low"]).replace(0, 1e-9)
            mf_multiplier = (2 * recent_df["close"] - recent_df["high"] - recent_df["low"]) / denom
            mf_volume = mf_multiplier * recent_df["volume"]
            result["accum_value"] = int((mf_volume * recent_df["close"]).sum())
        else:
            result["accum_value"] = 0
    else:
        result["avg_price_accum"] = 0
        result["accum_value"] = 0

    return result


def _classify_trend(series: pd.Series, period: int) -> str:
    """
    Classify trend direction using linear regression slope.
    Returns: 'rising', 'falling', or 'flat'.
    """
    recent = series.dropna().tail(period)
    if len(recent) < period // 2:
        return "unknown"

    values = recent.values
    x = np.arange(len(values))

    # Linear regression slope
    try:
        slope = np.polyfit(x, values, 1)[0]
    except (np.linalg.LinAlgError, ValueError):
        return "flat"

    # Normalize slope relative to mean value
    mean_val = np.mean(np.abs(values))
    if mean_val == 0:
        return "flat"

    normalized_slope = slope / mean_val * 100

    if normalized_slope > 0.5:
        return "rising"
    elif normalized_slope < -0.5:
        return "falling"
    else:
        return "flat"


def _categorize_mfi(mfi_value: float) -> str:
    """Categorize MFI level."""
    if mfi_value <= 20:
        return "oversold"
    elif mfi_value >= 80:
        return "overbought"
    elif mfi_value <= 40:
        return "weak"
    elif mfi_value >= 60:
        return "strong"
    else:
        return "neutral"


def _determine_phase(result: dict) -> str:
    """Determine overall accumulation/distribution phase."""
    adl = result.get("adl_trend", "unknown")
    obv = result.get("obv_trend", "unknown")
    mfi = result.get("mfi_level", "unknown")
    vol_ratio = result.get("volume_ratio")
    price_trend = result.get("price_trend", "unknown")

    # Strong accumulation signals
    bullish_signals = 0
    bearish_signals = 0

    if adl == "rising":
        bullish_signals += 2
    elif adl == "falling":
        bearish_signals += 2

    if obv == "rising":
        bullish_signals += 2
    elif obv == "falling":
        bearish_signals += 2

    if mfi in ("oversold", "weak"):
        bullish_signals += 1  # Oversold = potential accumulation opportunity
    elif mfi == "overbought":
        bearish_signals += 1

    if vol_ratio and vol_ratio > 1.5:
        bullish_signals += 1  # High volume = institutional activity

    # Price-volume divergence check
    if price_trend == "rising" and obv == "falling":
        bearish_signals += 2  # Distribution: price up but OBV down

    if price_trend == "falling" and obv == "rising":
        bullish_signals += 2  # Accumulation: price down but OBV up

    if bullish_signals >= 4 and bullish_signals > bearish_signals:
        return "accumulation"
    elif bearish_signals >= 4 and bearish_signals > bullish_signals:
        return "distribution"
    else:
        return "neutral"


def _phase_confidence(result: dict) -> float:
    """Calculate confidence level for the phase determination."""
    phase = result.get("phase", "neutral")
    if phase == "neutral":
        return 0.3

    score = 0.5  # Base score

    adl = result.get("adl_trend", "unknown")
    obv = result.get("obv_trend", "unknown")

    if phase == "accumulation":
        if adl == "rising":
            score += 0.15
        if obv == "rising":
            score += 0.15
        if result.get("mfi_level") in ("oversold", "weak"):
            score += 0.1
        if result.get("volume_ratio") and result["volume_ratio"] > 1.5:
            score += 0.1
    elif phase == "distribution":
        if adl == "falling":
            score += 0.15
        if obv == "falling":
            score += 0.15
        if result.get("mfi_level") == "overbought":
            score += 0.1

    return min(round(score, 3), 1.0)
