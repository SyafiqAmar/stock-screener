"""
Confidence score calculator for detected signals.
Each signal gets a score 0.0–1.0 based on multiple factors.
"""
import pandas as pd

from backend.config import SCORE_WEIGHTS, TIMEFRAME_SCORE_WEIGHTS


def calculate_confidence(signal: dict, df: pd.DataFrame, timeframe: str) -> float:
    """
    Calculate confidence score (0.0–1.0) for a signal.

    Scoring factors:
        1. Divergence strength (price vs indicator disagreement)
        2. Multi-indicator confirmation
        3. Volume confirmation at signal point
        4. Timeframe weight (higher TF = higher confidence)
        5. Proximity to current bar (recent = better)
        6. ADL phase alignment
        7. Fibonacci precision (for ABC patterns)
    """
    score = 0.0
    signal_type = signal.get("type", "")

    # ── 1. Timeframe weight ──
    tf_score = TIMEFRAME_SCORE_WEIGHTS.get(timeframe, 0.5)
    score += SCORE_WEIGHTS["timeframe_weight"] * tf_score

    # ── 2. Divergence strength ──
    if signal_type in ("bullish_divergence", "hidden_bullish_divergence"):
        div_strength = signal.get("divergence_strength", 0.5)
        score += SCORE_WEIGHTS["divergence_strength"] * min(div_strength, 1.0)

    # ── 3. Volume confirmation ──
    vol_score = _check_volume_confirmation(signal, df)
    score += SCORE_WEIGHTS["volume_confirmation"] * vol_score

    # ── 4. Proximity to current bar ──
    prox_score = _check_proximity(signal, df)
    score += SCORE_WEIGHTS["proximity"] * prox_score

    # ── 5. Fibonacci precision (ABC patterns) ──
    if signal_type == "abc_correction":
        fib_precision = signal.get("fibonacci_precision", 0.5)
        score += SCORE_WEIGHTS["fibonacci_precision"] * fib_precision
        # ABC patterns also get a base divergence_strength score
        score += SCORE_WEIGHTS["divergence_strength"] * 0.7  # Solid pattern = base score

    # ── 6. ADL alignment ──
    adl_score = _check_adl_alignment(signal, df)
    score += SCORE_WEIGHTS["adl_alignment"] * adl_score

    # ── 7. Multi-indicator confirmation (filled in by ranker later if applicable) ──
    multi_ind = signal.get("multi_indicator_bonus", 0)
    score += SCORE_WEIGHTS["multi_indicator_confirm"] * multi_ind

    # Accumulation/distribution signals get their own scoring
    if signal_type in ("accumulation", "distribution"):
        additional_data = signal.get("metadata", {}) or signal.get("additional_data", {})
        phase_conf = additional_data.get("phase_confidence", 0.5)
        score = 0.3 + (0.7 * phase_conf)  # Base + phase confidence

    return round(min(max(score, 0.0), 1.0), 4)


def _check_volume_confirmation(signal: dict, df: pd.DataFrame) -> float:
    """Check if volume supports the signal (increasing volume at signal bar)."""
    if "volume" not in df.columns or df["volume"].isna().all():
        return 0.5  # Neutral if no volume data

    bar_idx = signal.get("bar_index")
    if bar_idx is None or bar_idx >= len(df):
        return 0.5

    try:
        vol_avg = df["volume"].rolling(20).mean()
        if bar_idx < len(vol_avg) and pd.notna(vol_avg.iloc[bar_idx]) and vol_avg.iloc[bar_idx] > 0:
            ratio = df["volume"].iloc[bar_idx] / vol_avg.iloc[bar_idx]
            if ratio > 1.5:
                return 1.0  # Strong volume confirmation
            elif ratio > 1.0:
                return 0.7
            else:
                return 0.4
    except (IndexError, KeyError):
        pass

    return 0.5


def _check_proximity(signal: dict, df: pd.DataFrame) -> float:
    """Score based on how recent the signal is (closer to current bar = better)."""
    bar_idx = signal.get("bar_index")
    if bar_idx is None:
        return 0.5

    total_bars = len(df)
    if total_bars == 0:
        return 0.5

    # Distance from signal to the last bar
    distance = total_bars - bar_idx - 1
    if distance <= 3:
        return 1.0  # Very recent
    elif distance <= 10:
        return 0.8
    elif distance <= 20:
        return 0.5
    elif distance <= 40:
        return 0.3
    else:
        return 0.1  # Old signal


def _check_adl_alignment(signal: dict, df: pd.DataFrame) -> float:
    """Check if ADL trend aligns with the signal direction."""
    signal_type = signal.get("type", "")

    # Bullish signals should align with accumulation (ADL rising)
    if signal_type in ("bullish_divergence", "hidden_bullish_divergence", "abc_correction"):
        if "adl" in df.columns and df["adl"].notna().sum() > 20:
            adl_recent = df["adl"].tail(20)
            adl_slope = (adl_recent.iloc[-1] - adl_recent.iloc[0])
            mean_adl = abs(adl_recent.mean()) or 1
            normalized = adl_slope / mean_adl

            if normalized > 0.01:
                return 1.0  # ADL rising = aligned with bullish signal
            elif normalized > -0.01:
                return 0.5  # Flat = neutral
            else:
                return 0.2  # ADL falling = contradicts bullish signal

    return 0.5  # Default neutral
