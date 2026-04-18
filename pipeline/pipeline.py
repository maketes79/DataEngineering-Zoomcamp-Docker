from pathlib import Path
import sys

import pandas as pd
print("arguments", sys.argv)

if len(sys.argv) < 2:
    print("Usage: python pipeline/pipeline.py <month>")
    sys.exit(1)

month = int(sys.argv[1])

df = pd.DataFrame({"day": [1, 2], "number_passengers": [3, 4]})
df['month'] = month
print(df.head())

output_dir = Path(__file__).resolve().parent / "data"
output_dir.mkdir(exist_ok=True)

output_file = output_dir / f"number_passengers_month_{month}.parquet"
df.to_parquet(output_file, index=False)

print(f"Running pipeline for month {month}")
