"""
Elliott Wave ABC Correction pattern detection.
Uses ZigZag pivot algorithm + Fibonacci ratio validation.
"""
import logging
import pandas as pd

from backend.analysis.pivot_detector import compute_zigzag
from backend.config import (
    ABC_ZIGZAG_PCT,
    ABC_LOOKBACK,
    ABC_WAVE_B_RETRACE_MIN,
    ABC_WAVE_B_RETRACE_MAX,
    ABC_WAVE_C_EXTENSION_MIN,
    ABC_WAVE_C_EXTENSION_MAX,
)

logger = logging.getLogger(__name__)


def detect_abc_correction(
    df: pd.DataFrame,
    lookback: int | None = None,
    zigzag_pct: float | None = None,
) -> list[dict]:
    """
    Detect ABC Correction patterns using ZigZag + Fibonacci validation.

    Pattern structure:
        Peak(0) → Trough(A) → Peak(B) → Trough(C)

    Rules:
        1. There must be a prior uptrend (Peak0 is a significant high)
        2. Wave A: decline from Peak0 to Trough A
        3. Wave B: retracement of 38.2%–78.6% of Wave A
        4. Wave C: extension of 61.8%–161.8% of Wave A length
        5. Typically Wave C ends near or below Wave A's trough

    Args:
        df: DataFrame with 'close', 'high', 'low' columns (DatetimeIndex).
        lookback: Number of bars to analyze.
        zigzag_pct: ZigZag minimum swing percentage.

    Returns:
        List of signal dicts with wave coordinates and Fibonacci ratios.
    """
    lookback = lookback or ABC_LOOKBACK
    zigzag_pct = zigzag_pct or ABC_ZIGZAG_PCT

    if len(df) < 30:
        return []

    window = df.tail(lookback).copy()
    if len(window) < 20:
        return []

    # Compute ZigZag pivots from close price
    pivots = compute_zigzag(window["close"], pct_threshold=zigzag_pct)

    if len(pivots) < 4:
        return []

    signals = []

    # Scan for Peak → Trough → Peak → Trough sequences (ABC correction)
    for i in range(len(pivots) - 3):
        p0 = pivots[i]      # Peak before correction (end of impulse)
        pa = pivots[i + 1]  # End of Wave A (trough)
        pb = pivots[i + 2]  # End of Wave B (peak / retracement)
        pc = pivots[i + 3]  # End of Wave C (trough)

        # Validate direction: High → Low → High → Low
        if p0["type"] != "high" or pa["type"] != "low":
            continue
        if pb["type"] != "high" or pc["type"] != "low":
            continue

        # Wave A: decline
        wave_a_length = p0["price"] - pa["price"]
        if wave_a_length <= 0:
            continue  # Must be a decline

        # Wave B: retracement ratio (how much of Wave A did B retrace?)
        wave_b_retrace_abs = pb["price"] - pa["price"]
        if wave_b_retrace_abs <= 0:
            continue  # B must bounce up from A's trough

        wave_b_ratio = wave_b_retrace_abs / wave_a_length

        # Wave C: extension relative to Wave A
        wave_c_length = pb["price"] - pc["price"]
        if wave_c_length <= 0:
            continue  # C must decline from B's peak

        wave_c_ratio = wave_c_length / wave_a_length

        # ── Fibonacci validation ──
        b_valid = ABC_WAVE_B_RETRACE_MIN <= wave_b_ratio <= ABC_WAVE_B_RETRACE_MAX
        c_valid = ABC_WAVE_C_EXTENSION_MIN <= wave_c_ratio <= ABC_WAVE_C_EXTENSION_MAX

        if b_valid and c_valid:
            # Calculate how close the Fibonacci ratios are to ideal values
            # Ideal B retracement: 0.618, Ideal C extension: 1.0
            b_precision = 1.0 - abs(wave_b_ratio - 0.618) / 0.618
            c_precision = 1.0 - abs(wave_c_ratio - 1.0) / 1.0
            fib_precision = max(0, (b_precision + c_precision) / 2)

            # Check if correction is complete (Wave C near or below Wave A)
            correction_complete = pc["price"] <= pa["price"] * 1.02  # 2% tolerance

            signal = {
                "type": "abc_correction",
                "wave_a_start": {
                    "index": p0["index"],
                    "price": p0["price"],
                    "date": str(p0.get("date", "")),
                },
                "wave_a_end": {
                    "index": pa["index"],
                    "price": pa["price"],
                    "date": str(pa.get("date", "")),
                },
                "wave_b_end": {
                    "index": pb["index"],
                    "price": pb["price"],
                    "date": str(pb.get("date", "")),
                },
                "wave_c_end": {
                    "index": pc["index"],
                    "price": pc["price"],
                    "date": str(pc.get("date", "")),
                },
                "b_retracement": round(wave_b_ratio, 4),
                "c_extension": round(wave_c_ratio, 4),
                "fibonacci_precision": round(fib_precision, 4),
                "correction_complete": correction_complete,
                "wave_a_pct": round(wave_a_length / p0["price"] * 100, 2),
                "bar_index": pc["index"],
                "date": pc.get("date"),
            }
            signals.append(signal)
            logger.info(
                f"ABC Correction detected: B retrace={wave_b_ratio:.3f}, "
                f"C ext={wave_c_ratio:.3f}, complete={correction_complete}"
            )

    # Keep only the most recent ABC pattern
    if len(signals) > 1:
        signals = [max(signals, key=lambda s: s["bar_index"])]

    return signals
