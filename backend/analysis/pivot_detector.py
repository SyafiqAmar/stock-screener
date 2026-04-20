"""
Pivot (local extrema) detection using scipy.signal.find_peaks.
Used by divergence and Elliott wave detectors.
"""
import numpy as np
import pandas as pd
from scipy.signal import find_peaks


def find_local_minima(
    series: pd.Series,
    distance: int = 5,
    prominence: float | None = None,
) -> np.ndarray:
    """
    Find local minima (troughs) in a series.
    Uses find_peaks on the negated series.

    Args:
        series: Price or indicator series.
        distance: Minimum number of bars between troughs.
        prominence: Minimum prominence (how much the trough stands out).

    Returns:
        Array of indices where local minima occur.
    """
    clean = series.dropna()
    if len(clean) < distance * 2:
        return np.array([], dtype=int)

    kwargs = {"distance": distance}
    if prominence is not None:
        kwargs["prominence"] = prominence

    peaks, _ = find_peaks(-clean.values, **kwargs)

    # Map back to original series indices
    original_indices = clean.index
    if isinstance(original_indices, pd.RangeIndex):
        return peaks
    else:
        # For DatetimeIndex: return positional indices into the original series
        return peaks


def find_local_maxima(
    series: pd.Series,
    distance: int = 5,
    prominence: float | None = None,
) -> np.ndarray:
    """
    Find local maxima (peaks) in a series.

    Args:
        series: Price or indicator series.
        distance: Minimum number of bars between peaks.
        prominence: Minimum prominence.

    Returns:
        Array of indices where local maxima occur.
    """
    clean = series.dropna()
    if len(clean) < distance * 2:
        return np.array([], dtype=int)

    kwargs = {"distance": distance}
    if prominence is not None:
        kwargs["prominence"] = prominence

    peaks, _ = find_peaks(clean.values, **kwargs)
    return peaks


def get_pivot_pairs(indices: np.ndarray, min_bars_apart: int = 5) -> list[tuple[int, int]]:
    """
    Create consecutive pairs of pivots for comparison.

    Args:
        indices: Array of pivot indices.
        min_bars_apart: Minimum bars between pivots in a pair.

    Returns:
        List of (index1, index2) tuples.
    """
    pairs = []
    for i in range(len(indices) - 1):
        if indices[i + 1] - indices[i] >= min_bars_apart:
            pairs.append((int(indices[i]), int(indices[i + 1])))
    return pairs


def find_closest_pivot(
    pivot_indices: np.ndarray,
    target_index: int,
    max_lag: int = 3,
) -> int | None:
    """
    Find the closest pivot index to a target index within a lag window.
    Used to match indicator pivots to price pivots.

    Args:
        pivot_indices: Array of pivot indices.
        target_index: The index to find closest pivot to.
        max_lag: Maximum allowable distance.

    Returns:
        Closest pivot index, or None if none within max_lag.
    """
    if len(pivot_indices) == 0:
        return None

    distances = np.abs(pivot_indices - target_index)
    min_dist_idx = np.argmin(distances)

    if distances[min_dist_idx] <= max_lag:
        return int(pivot_indices[min_dist_idx])
    return None


def compute_zigzag(
    series: pd.Series,
    pct_threshold: float = 5.0,
) -> list[dict]:
    """
    Compute ZigZag pivots — connecting significant highs and lows.
    Filters out moves smaller than pct_threshold%.

    Args:
        series: Price series (typically 'close').
        pct_threshold: Minimum percentage move to register a new pivot.

    Returns:
        List of dicts: [{'index': int, 'price': float, 'type': 'high'|'low'}, ...]
    """
    if len(series) < 3:
        return []

    values = series.values
    pivots = []
    last_pivot_idx = 0
    last_pivot_val = values[0]
    last_pivot_type = None  # 'high' or 'low'

    for i in range(1, len(values)):
        pct_change = (values[i] - last_pivot_val) / last_pivot_val * 100

        if last_pivot_type is None:
            # Initialize direction
            if pct_change >= pct_threshold:
                pivots.append({
                    "index": last_pivot_idx,
                    "price": float(last_pivot_val),
                    "type": "low",
                    "date": series.index[last_pivot_idx] if hasattr(series.index, '__getitem__') else None,
                })
                last_pivot_idx = i
                last_pivot_val = values[i]
                last_pivot_type = "high"
            elif pct_change <= -pct_threshold:
                pivots.append({
                    "index": last_pivot_idx,
                    "price": float(last_pivot_val),
                    "type": "high",
                    "date": series.index[last_pivot_idx] if hasattr(series.index, '__getitem__') else None,
                })
                last_pivot_idx = i
                last_pivot_val = values[i]
                last_pivot_type = "low"
        elif last_pivot_type == "high":
            if values[i] > last_pivot_val:
                # Extend the high
                last_pivot_idx = i
                last_pivot_val = values[i]
            elif pct_change <= -pct_threshold:
                # Confirmed high, now looking for low
                pivots.append({
                    "index": last_pivot_idx,
                    "price": float(last_pivot_val),
                    "type": "high",
                    "date": series.index[last_pivot_idx] if hasattr(series.index, '__getitem__') else None,
                })
                last_pivot_idx = i
                last_pivot_val = values[i]
                last_pivot_type = "low"
        elif last_pivot_type == "low":
            if values[i] < last_pivot_val:
                # Extend the low
                last_pivot_idx = i
                last_pivot_val = values[i]
            elif pct_change >= pct_threshold:
                # Confirmed low, now looking for high
                pivots.append({
                    "index": last_pivot_idx,
                    "price": float(last_pivot_val),
                    "type": "low",
                    "date": series.index[last_pivot_idx] if hasattr(series.index, '__getitem__') else None,
                })
                last_pivot_idx = i
                last_pivot_val = values[i]
                last_pivot_type = "high"

    # Append the last pivot
    if last_pivot_type:
        pivots.append({
            "index": last_pivot_idx,
            "price": float(last_pivot_val),
            "type": last_pivot_type,
            "date": series.index[last_pivot_idx] if hasattr(series.index, '__getitem__') else None,
        })

    return pivots
