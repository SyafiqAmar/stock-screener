import pandas as pd
import numpy as np
import json

df = pd.DataFrame({
    'date': [pd.Timestamp('2026-04-21')],
    'rsi_14': [np.nan],
    'macd': [1.2]
})

df['time'] = df['date'].astype('int64') // 10**9
records = df.to_dict('records')

try:
    json.dumps(records)
    print("Serialization success")
except Exception as e:
    print(f"Serialization failed: {e}")

# Fix
df_fixed = df.replace({np.nan: None})
records_fixed = df_fixed.to_dict('records')
try:
    json.dumps(records_fixed)
    print("Fixed serialization success")
    print(records_fixed)
except Exception as e:
    print(f"Fixed serialization failed: {e}")
