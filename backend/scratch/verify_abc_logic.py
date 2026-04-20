import pandas as pd
import numpy as np
from backend.analysis.trade_setup import calculate_trade_setup

def test_abc_targets():
    # Mock data
    df = pd.DataFrame({
        "open": [100]*10,
        "high": [110]*10,
        "low": [90]*10,
        "close": [105]*10
    })
    
    # Bullish ABC: A (110->90), B (90->100), C (100->85)
    # Wave A Start: 110
    # Wave A End: 90 (pa_price)
    # Wave B End: 100 (pb_price)
    # Wave C End: 85 (pc_price)
    
    signal = {
        "type": "abc_correction",
        "wave_a_start": {"price": 110},
        "wave_a_end": {"price": 90},
        "wave_b_end": {"price": 100},
        "wave_c_end": {"price": 85},
        "bar_index": 9
    }
    
    setup = calculate_trade_setup(signal, df)
    
    wave_a_len = 110 - 90 # 20
    expected_tp1 = 85 + (20 * 1.272) # 85 + 25.44 = 110.44
    expected_tp2 = 85 + (20 * 1.618) # 85 + 32.36 = 117.36
    
    print(f"Signal: ABC")
    print(f"Wave A Len: {wave_a_len}")
    print(f"Calculated TP1: {setup['target_1']} (Expected: {expected_tp1:.2f})")
    print(f"Calculated TP2: {setup['target_2']} (Expected: {expected_tp2:.2f})")
    print(f"Entry: {setup['entry']}")
    print(f"Risk/Reward: {setup['risk_reward_1']}")

if __name__ == "__main__":
    test_abc_targets()
