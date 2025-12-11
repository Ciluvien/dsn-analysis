#!/usr/bin/env python3

from extract import query_prometheus_CSV
import polars as pl
from os import path

DATA_DIR = path.abspath(path.join(path.dirname(__file__),"../../data/"))
OUT_FILE = path.join(DATA_DIR, "data_rate_export.parquet")

if __name__=="__main__":
    q_data_rate_dsn = f'signal_data_rate_b_per_s'
    df = query_prometheus_CSV("http://localhost:9090",q_data_rate_dsn, "2025-06-01T00:00:00Z", "2025-11-01T00:00:00Z","5s")
    # df = df.select(["Time","signal_data_rate_b_per_s","dish_name","station_name","target_id","target_name"])

    print(df.head(10).collect())

    df.sink_parquet(OUT_FILE)
