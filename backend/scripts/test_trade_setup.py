import sys
from pathlib import Path
import pandas as pd

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from backend.analysis.trade_setup import calculate_trade_setup

def test_divergence_setup():
    print("Testing Bullish Divergence Setup...")
    # Create dummy DataFrame
    df = pd.DataFrame({
        "open":  [100, 102, 105, 103, 100, 98, 101],
        "high":  [110, 112, 115, 113, 110, 108, 111],
        "low":   [90,  92,  95,  93,  90,  88,  91],
        "close": [105, 103, 100, 95,  98,  102, 108], # Entry is 108
    })
    
    # HH=115 (index 2), LL=88 (index 5)
    # Range = 115 - 88 = 27
    # TP1 (0.5) = 88 + 13.5 = 101.5
    # TP2 (0.618) = 88 + 16.686 = 104.686
    # SL should be low of index 6 (108>102), which is 91.
    
    signal = {
        "type": "bullish_divergence",
        "pivot_1_bar": 1,
        "pivot_2_bar": 5,
        "bar_index": 6
    }
    
    setup = calculate_trade_setup(signal, df)
    print("Setup:", setup)
    
    assert setup["stop_loss"] == 91.0
    assert setup["target_1"] == 101.5
    assert setup["target_2"] == 104.69 # Rounded 0.618
    print("Test passed!")

if __name__ == "__main__":
    try:
        test_divergence_setup()
    except Exception as e:
        print("Test failed:", e)
        sys.exit(1)
