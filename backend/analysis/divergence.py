"""
Divergence detection engine.
Detects Bullish Divergence and Hidden Bullish Divergence using
price vs indicator pivot comparison.
"""
import logging
import pandas as pd
import numpy as np

from backend.analysis.pivot_detector import (
    find_local_minima,
    get_pivot_pairs,
    find_closest_pivot,
)
from backend.config import (
    DIVERGENCE_LOOKBACK,
    DIVERGENCE_MIN_PIVOT_DISTANCE,
    DIVERGENCE_MAX_LAG,
    DIVERGENCE_MIN_INDICATOR_DELTA,
    DIVERGENCE_MIN_PRICE_PCT,
)

logger = logging.getLogger(__name__)


def detect_bullish_divergence(
    df: pd.DataFrame,
    indicator: str = "rsi_14",
    lookback: int | None = None,
    min_distance: int | None = None,
    max_lag: int | None = None,
    min_ind_delta: float | None = None,
) -> list[dict]:
    """
    Detect Regular Bullish Divergence:
        - Price makes a LOWER LOW
        - Indicator makes a HIGHER LOW
    """
    lookback = lookback or DIVERGENCE_LOOKBACK
    min_distance = min_distance or DIVERGENCE_MIN_PIVOT_DISTANCE
    max_lag = max_lag or DIVERGENCE_MAX_LAG
    min_ind_delta = min_ind_delta or DIVERGENCE_MIN_INDICATOR_DELTA

    if indicator not in df.columns:
        return []

    window = df.tail(lookback).copy()
    if len(window) < min_distance * 3:
        return []

    price_series = window["low"].reset_index(drop=True)
    ind_series = window[indicator].dropna()
    if len(ind_series) < min_distance * 3:
        return []
    ind_series = ind_series.reset_index(drop=True)

    price_lows = find_local_minima(price_series, distance=min_distance)
    ind_lows = find_local_minima(ind_series, distance=min_distance)

    if len(price_lows) < 2:
        return []

    signals = []
    pairs = get_pivot_pairs(price_lows, min_bars_apart=min_distance)

    for p1, p2 in pairs:
        if price_series.iloc[p2] >= price_series.iloc[p1]:
            continue

        ind_at_p1 = find_closest_pivot(ind_lows, p1, max_lag=max_lag)
        ind_at_p2 = find_closest_pivot(ind_lows, p2, max_lag=max_lag)

        if ind_at_p1 is None or ind_at_p2 is None:
            if p1 < len(ind_series) and p2 < len(ind_series):
                ind_val_p1 = ind_series.iloc[p1]
                ind_val_p2 = ind_series.iloc[p2]
                if pd.notna(ind_val_p1) and pd.notna(ind_val_p2):
                    if ind_val_p2 > ind_val_p1:
                        signals.append(_build_signal(
                            signal_type="bullish_divergence",
                            indicator=indicator,
                            p1=p1, p2=p2,
                            price_p1=float(price_series.iloc[p1]),
                            price_p2=float(price_series.iloc[p2]),
                            ind_p1=p1, ind_p2=p2,
                            ind_val_p1=float(ind_val_p1),
                            ind_val_p2=float(ind_val_p2),
                            window=window,
                            lookback=lookback,
                        ))
            continue

        # Kedua baris ini harus ada — assign p1 DAN p2
        ind_val_p1 = ind_series.iloc[ind_at_p1]
        ind_val_p2 = ind_series.iloc[ind_at_p2]

        if pd.notna(ind_val_p1) and pd.notna(ind_val_p2) and ind_val_p2 >= (ind_val_p1 + min_ind_delta):
            signals.append(_build_signal(
                signal_type="bullish_divergence",
                indicator=indicator,
                p1=p1, p2=p2,
                price_p1=float(price_series.iloc[p1]),
                price_p2=float(price_series.iloc[p2]),
                ind_p1=ind_at_p1, ind_p2=ind_at_p2,
                ind_val_p1=float(ind_val_p1),
                ind_val_p2=float(ind_val_p2),
                window=window,
                lookback=lookback,
            ))

    if len(signals) > 1:
        signals = [max(signals, key=lambda s: s["bar_index"])]

    return signals


def detect_hidden_bullish_divergence(
    df: pd.DataFrame,
    indicator: str = "rsi_14",
    lookback: int | None = None,
    min_distance: int | None = None,
    max_lag: int | None = None,
    min_ind_delta: float | None = None,
) -> list[dict]:
    """
    Detect Hidden Bullish Divergence:
        - Price makes a HIGHER LOW (uptrend intact)
        - Indicator makes a LOWER LOW (false weakness signal)
    """
    lookback = lookback or DIVERGENCE_LOOKBACK
    min_distance = min_distance or DIVERGENCE_MIN_PIVOT_DISTANCE
    max_lag = max_lag or DIVERGENCE_MAX_LAG
    min_ind_delta = min_ind_delta or DIVERGENCE_MIN_INDICATOR_DELTA

    if indicator not in df.columns:
        return []

    window = df.tail(lookback).copy()
    if len(window) < min_distance * 3:
        return []

    price_series = window["low"].reset_index(drop=True)
    ind_series = window[indicator].dropna()
    if len(ind_series) < min_distance * 3:
        return []
    ind_series = ind_series.reset_index(drop=True)

    price_lows = find_local_minima(price_series, distance=min_distance)
    ind_lows = find_local_minima(ind_series, distance=min_distance)

    if len(price_lows) < 2:
        return []

    signals = []
    pairs = get_pivot_pairs(price_lows, min_bars_apart=min_distance)

    for p1, p2 in pairs:
        if price_series.iloc[p2] <= price_series.iloc[p1]:
            continue

        ind_at_p1 = find_closest_pivot(ind_lows, p1, max_lag=max_lag)
        ind_at_p2 = find_closest_pivot(ind_lows, p2, max_lag=max_lag)

        if ind_at_p1 is None or ind_at_p2 is None:
            if p1 < len(ind_series) and p2 < len(ind_series):
                ind_val_p1 = ind_series.iloc[p1]
                ind_val_p2 = ind_series.iloc[p2]
                if pd.notna(ind_val_p1) and pd.notna(ind_val_p2):
                    if ind_val_p2 < ind_val_p1:
                        signals.append(_build_signal(
                            signal_type="hidden_bullish_divergence",
                            indicator=indicator,
                            p1=p1, p2=p2,
                            price_p1=float(price_series.iloc[p1]),
                            price_p2=float(price_series.iloc[p2]),
                            ind_p1=p1, ind_p2=p2,
                            ind_val_p1=float(ind_val_p1),
                            ind_val_p2=float(ind_val_p2),
                            window=window,
                            lookback=lookback,
                        ))
            continue

        # FIXED: kedua baris assign ind_val_p1 dan ind_val_p2 harus ada
        ind_val_p1 = ind_series.iloc[ind_at_p1]   # ← baris ini hilang di versi kamu
        ind_val_p2 = ind_series.iloc[ind_at_p2]

        if pd.notna(ind_val_p1) and pd.notna(ind_val_p2) and ind_val_p2 <= (ind_val_p1 - min_ind_delta):
            signals.append(_build_signal(
                signal_type="hidden_bullish_divergence",
                indicator=indicator,
                p1=p1, p2=p2,
                price_p1=float(price_series.iloc[p1]),
                price_p2=float(price_series.iloc[p2]),
                ind_p1=ind_at_p1, ind_p2=ind_at_p2,
                ind_val_p1=float(ind_val_p1),
                ind_val_p2=float(ind_val_p2),
                window=window,
                lookback=lookback,
            ))

    if len(signals) > 1:
        signals = [max(signals, key=lambda s: s["bar_index"])]

    return signals


def _build_signal(
    signal_type: str,
    indicator: str,
    p1: int, p2: int,
    price_p1: float, price_p2: float,
    ind_p1: int, ind_p2: int,
    ind_val_p1: float, ind_val_p2: float,
    window: pd.DataFrame,
    lookback: int,
) -> dict:
    """Build a standardized signal dict."""
    price_delta_pct = abs(price_p2 - price_p1) / price_p1 * 100
    ind_delta_pct = abs(ind_val_p2 - ind_val_p1) / max(abs(ind_val_p1), 0.01) * 100
    divergence_strength = min((price_delta_pct + ind_delta_pct) / 20, 1.0)

    date_idx = window.index[min(p2, len(window) - 1)] if p2 < len(window) else window.index[-1]

    return {
        "type": signal_type,
        "indicator": indicator,
        "price_pivot_1": price_p1,
        "price_pivot_2": price_p2,
        "ind_pivot_1_val": round(ind_val_p1, 4),
        "ind_pivot_2_val": round(ind_val_p2, 4),
        "pivot_1_bar": p1,
        "pivot_2_bar": p2,
        "divergence_strength": round(divergence_strength, 4),
        "bar_index": p2,
        "date": date_idx,
    }