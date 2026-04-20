"""
Trade Setup Calculator.
Computes Entry, Stop Loss, and Take Profit levels based on technical patterns.

PERBAIKAN BUG:
1. SL divergence: dulu pakai bar_index (trough), sekarang scan mundur dari bar TERAKHIR
2. TP divergence: HH dicari dari seluruh range antara P1 sampai awal window, bukan hanya 20 bar
3. TP2 ABC: wave_a_len dihitung dari metadata yang sudah flatten, dengan validasi positif
"""
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def calculate_trade_setup(signal: dict, df: pd.DataFrame) -> dict:
    """Main entry point untuk kalkulasi trade setup."""
    sig_type = signal.get("type", "")

    if sig_type in ("bullish_divergence", "hidden_bullish_divergence"):
        return _setup_for_divergence(signal, df)
    elif sig_type == "abc_correction":
        return _setup_for_abc(signal, df)

    return {}


def _setup_for_divergence(signal: dict, df: pd.DataFrame) -> dict:
    """
    Trade setup untuk Bullish Divergence & Hidden Bullish Divergence.

    Entry  : Close bar TERAKHIR (bukan bar pivot)
    SL     : Low candle hijau terdekat, scan mundur dari bar TERAKHIR
             (bukan dari bar_index/trough yang sudah lama)
    TP1    : LL + 50% range (HH - LL)
    TP2    : LL + 61.8% range (HH - LL)

    PERBAIKAN:
    - Sebelumnya entry diambil dari bar_index (posisi trough/lows), yang menyebabkan
      SL dicari di area bawah dan bisa LEBIH TINGGI dari harga entry terkini.
    - Sekarang entry = close bar terakhir, SL = low green candle terbaru.
    - HH dicari dari seluruh range P1 ke belakang, bukan hanya 20 bar.
    """
    try:
        last_bar = len(df) - 1
        p1_bar   = signal.get("pivot_1_bar", 0)
        p2_bar   = signal.get("pivot_2_bar", last_bar)

        # ── Entry: close bar TERAKHIR ──────────────────────────────────
        entry = float(df["close"].iloc[last_bar])

        # ── SL: low candle hijau, scan mundur dari bar TERAKHIR ─────────
        # Cari candle hijau (close > open) dari bar terakhir mundur max 15 bar
        sl_price = None
        for i in range(last_bar, max(-1, last_bar - 15), -1):
            if df["close"].iloc[i] > df["open"].iloc[i]:
                sl_price = float(df["low"].iloc[i])
                break

        # Fallback: 2% di bawah low bar terakhir
        if sl_price is None:
            sl_price = float(df["low"].iloc[last_bar]) * 0.98

        # Pastikan SL selalu di BAWAH entry
        if sl_price >= entry:
            sl_price = entry * 0.98   # paksa 2% di bawah entry

        # ── TP: range dari HH ke LL ────────────────────────────────────
        # LL adalah low di pivot P2 (trough signal)
        p2_idx  = min(p2_bar, last_bar)
        ll_price = float(df["low"].iloc[p2_idx])

        # HH: cari highest high dari awal window sampai P1
        # (seluruh range sebelum divergence, bukan hanya 20 bar)
        hh_end   = min(p1_bar + 1, len(df))
        hh_start = max(0, hh_end - 60)   # max 60 bar lookback untuk HH
        hh_range = df.iloc[hh_start:hh_end]
        if not hh_range.empty:
            hh_price = float(hh_range["high"].max())
        else:
            hh_price = float(df["high"].iloc[p1_bar]) if p1_bar < len(df) else entry * 1.1

        # Pastikan HH > LL untuk range yang valid
        price_range = hh_price - ll_price
        if price_range <= 0:
            # Fallback: gunakan ATR-like range dari 20 bar terakhir
            price_range = float(
                (df["high"].tail(20) - df["low"].tail(20)).mean()
            )
        if price_range <= 0:
            price_range = entry * 0.05   # 5% dari entry sebagai minimum range

        tp1 = ll_price + (price_range * 0.500)   # 50% retracement
        tp2 = ll_price + (price_range * 0.618)   # 61.8% retracement

        # Pastikan TP selalu DI ATAS entry
        if tp1 <= entry:
            tp1 = entry * 1.03   # minimum 3% di atas entry
        if tp2 <= tp1:
            tp2 = tp1 * 1.02    # TP2 selalu di atas TP1

        risk   = entry - sl_price
        reward = tp1 - entry
        rr1    = round(reward / risk, 2) if risk > 0 else 0

        return {
            "entry":        round(entry,    2),
            "stop_loss":    round(sl_price, 2),
            "target_1":     round(tp1,      2),
            "target_2":     round(tp2,      2),
            "risk_reward_1": rr1,
            "patterns": {
                "hh": round(hh_price,  2),
                "ll": round(ll_price,  2),
                "range": round(price_range, 2),
            },
        }

    except Exception as e:
        logger.error(f"Error calculating divergence trade setup: {e}")
        return {}


def _setup_for_abc(signal: dict, df: pd.DataFrame) -> dict:
    """
    Trade setup untuk ABC Correction.

    Entry  : Close bar terakhir (atau close mendekati Wave C)
    SL     : 2% di bawah Wave C (trough akhir koreksi)
    TP1    : Wave B (puncak retracement — target konservatif)
    TP2    : Wave C + 61.8% dari panjang Wave A (target agresif)

    PERBAIKAN:
    - wave_a_len sebelumnya bisa 0 atau negatif karena wave_a_start.price
      tidak selalu ada setelah JSON serialization di database.
    - Sekarang dicoba dari metadata langsung, dengan fallback ke perhitungan
      dari wave_a_end dan wave_c_end sebagai proxy.
    - Validasi: TP1 dan TP2 harus selalu di atas entry.
    """
    try:
        # Ambil harga pivot dari metadata signal
        wave_a_start = signal.get("wave_a_start", {}) or {}
        wave_a_end   = signal.get("wave_a_end",   {}) or {}
        wave_b_end   = signal.get("wave_b_end",   {}) or {}
        wave_c_end   = signal.get("wave_c_end",   {}) or {}

        pc_price = wave_c_end.get("price")   # trough C (titik entry ideal)
        pb_price = wave_b_end.get("price")   # puncak B (TP1 konservatif)
        pa_price = wave_a_end.get("price")   # trough A
        p0_price = wave_a_start.get("price") # puncak sebelum koreksi

        if not all([pc_price, pb_price, pa_price]):
            logger.warning("ABC setup: missing pivot prices in signal metadata")
            return {}

        # ── Entry: close bar terakhir ──────────────────────────────────
        last_bar = len(df) - 1
        entry = float(df["close"].iloc[last_bar])

        # ── SL: 2% di bawah Wave C (lowest point koreksi) ─────────────
        sl = round(float(pc_price) * 0.98, 2)

        # Pastikan SL di bawah entry
        if sl >= entry:
            sl = round(entry * 0.98, 2)

        # ── TP1: Wave B (target konservatif) ──────────────────────────
        tp1 = round(float(pb_price), 2)

        # Pastikan TP1 di atas entry
        if tp1 <= entry:
            tp1 = round(entry * 1.05, 2)   # fallback: 5% di atas entry

        # ── TP2: Wave C + 61.8% panjang Wave A ────────────────────────
        # Panjang Wave A = P0 (peak) - PA (trough A)
        if p0_price and float(p0_price) > float(pa_price):
            wave_a_len = float(p0_price) - float(pa_price)
        else:
            # Fallback: estimasi Wave A dari Wave B ratio (biasanya 0.5-0.8 dari A)
            b_ratio = signal.get("b_retracement", 0.618)
            if b_ratio and b_ratio > 0:
                wave_a_len = (float(pb_price) - float(pc_price)) / max(b_ratio, 0.1)
            else:
                wave_a_len = float(pb_price) - float(pc_price)

        # Validasi wave_a_len harus positif
        if wave_a_len <= 0:
            wave_a_len = abs(float(pb_price) - float(pc_price))

        tp2 = round(float(pc_price) + (wave_a_len * 0.618), 2)

        # Pastikan TP2 > TP1 > entry
        if tp2 <= tp1:
            tp2 = round(tp1 * 1.03, 2)
        if tp2 <= entry:
            tp2 = round(tp1 * 1.05, 2)

        # ── Risk/Reward ────────────────────────────────────────────────
        risk   = entry - sl
        reward = tp1 - entry
        rr1    = round(reward / risk, 2) if risk > 0 else 0

        return {
            "entry":         round(entry, 2),
            "stop_loss":     sl,
            "target_1":      tp1,
            "target_2":      tp2,
            "risk_reward_1": rr1,
        }

    except Exception as e:
        logger.error(f"Error calculating ABC trade setup: {e}")
        return {}