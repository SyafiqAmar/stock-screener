"""
Trade Setup Calculator.
Computes Entry, Stop Loss, and Take Profit levels based on technical patterns.
"""
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def calculate_trade_setup(signal: dict, df: pd.DataFrame) -> dict:
    """
    Main entry point for trade setup calculation.
    """
    sig_type = signal.get("type", "")
    
    if sig_type in ("bullish_divergence", "hidden_bullish_divergence"):
        return _setup_for_divergence(signal, df)
    elif sig_type == "abc_correction":
        return _setup_for_abc(signal, df)
    
    return {}

def _setup_for_divergence(signal: dict, df: pd.DataFrame) -> dict:
    """
    Calculate levels for Bullish Divergence.
    SL: Low of the last green candle.
    TP: Measured from HH (before P1) to LL (P2). 
        TP1 = 0.5, TP2 = 0.618
    """
    try:
        # 1. SL: Last Green Candle Low
        # Scan back from the signal bar index
        bar_idx = signal.get("bar_index")
        if bar_idx is None or bar_idx >= len(df):
            bar_idx = len(df) - 1
            
        sl_price = None
        # look back up to 10 bars for a green candle
        for i in range(bar_idx, max(-1, bar_idx - 10), -1):
            if df["close"].iloc[i] > df["open"].iloc[i]:
                sl_price = float(df["low"].iloc[i])
                break
        
        # Fallback for SL if no green candle found: 2% below the LL
        if not sl_price:
            sl_price = float(df["low"].iloc[bar_idx] * 0.98)

        # 2. TP: Range from HH to LL
        # LL is at pivot_2_bar (original divergence logic)
        p1_bar = signal.get("pivot_1_bar", 0)
        p2_bar = signal.get("pivot_2_bar", bar_idx)
        
        # Higher High (HH): Highest peak before P1 (look back 20 bars)
        hh_search_start = max(0, p1_bar - 20)
        hh_range = df.iloc[hh_search_start:p1_bar+1]
        if not hh_range.empty:
            hh_price = float(hh_range["high"].max())
        else:
            hh_price = float(df["high"].iloc[p1_bar])
            
        ll_price = float(df["low"].iloc[p2_bar])
        price_range = hh_price - ll_price
        
        if price_range <= 0:
            price_range = ll_price * 0.05 # Fallback relative range

        tp1 = ll_price + (price_range * 0.5)
        tp2 = ll_price + (price_range * 0.618)
        
        entry = float(df["close"].iloc[bar_idx]) if bar_idx < len(df) else 0

        return {
            "entry": round(entry, 2),
            "stop_loss": round(sl_price, 2),
            "target_1": round(tp1, 2),
            "target_2": round(tp2, 2),
            "risk_reward_1": round((tp1 - entry) / max(entry - sl_price, 1), 2),
            "patterns": {
                "hh": round(hh_price, 2),
                "ll": round(ll_price, 2)
            }
        }
    except Exception as e:
        logger.error(f"Error calculating divergence trade setup: {e}")
        return {}

def _setup_for_abc(signal: dict, df: pd.DataFrame) -> dict:
    """
    Calculate levels for ABC Correction.
    SL: Below Pivot C.
    TP1: Pivot B.
    TP2: 1.618 Extension from C.
    """
    try:
        pc_price = signal.get("wave_c_end", {}).get("price")
        pb_price = signal.get("wave_b_end", {}).get("price")
        pa_price = signal.get("wave_a_end", {}).get("price")
        
        if not all([pc_price, pb_price, pa_price]):
            return {}

        sl = pc_price * 0.98
        tp1 = pb_price
        
        # Wave C target extension
        wave_a_len = signal.get("wave_a_start", {}).get("price", 0) - pa_price
        tp2 = pc_price + (wave_a_len * 0.618) # 61.8% of Wave A added to C
        
        bar_idx = signal.get("bar_index", len(df)-1)
        entry = float(df["close"].iloc[bar_idx]) if bar_idx < len(df) else pc_price

        return {
            "entry": round(entry, 2),
            "stop_loss": round(sl, 2),
            "target_1": round(tp1, 2),
            "target_2": round(tp2, 2),
            "risk_reward_1": round((tp1 - entry) / max(entry - sl, 1), 2)
        }
    except Exception as e:
        logger.error(f"Error calculating ABC trade setup: {e}")
        return {}
