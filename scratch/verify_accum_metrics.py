import pandas as pd
import numpy as np
from backend.analysis.accumulation import analyze_accumulation_distribution

# Mock data
df = pd.DataFrame({
    'date': pd.date_range(start='2026-01-01', periods=30, freq='D'),
    'open': np.random.uniform(100, 110, 30),
    'high': np.random.uniform(111, 120, 30),
    'low': np.random.uniform(90, 99, 30),
    'close': np.random.uniform(100, 110, 30),
    'volume': np.random.uniform(1000000, 5000000, 30),
    'adl': np.linspace(1000, 5000, 30),
    'obv': np.linspace(100000, 500000, 30),
    'mfi': np.random.uniform(20, 80, 30)
}).set_index('date')

result = analyze_accumulation_distribution(df)
print("Analysis Result:")
import json
print(json.dumps(result, indent=2))

if 'accum_value' in result and 'avg_price_accum' in result:
    print("\n✅ New metrics found in analysis result!")
else:
    print("\n❌ New metrics MISSING in analysis result!")
