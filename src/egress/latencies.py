#!/usr/bin/env python3

from extract import query_prometheus_CSV
import polars as pl
from os import path

DATA_DIR = path.abspath(path.join(path.dirname(__file__),"../../data/"))
OUT_FILE = path.join(DATA_DIR, "latency_export.parquet")

if __name__=="__main__":
    q_range_dsn = f'target_round_trip_seconds{{data_source=~"DSN Now", dish_activity=~".*Tracking.*"}}'
    df = query_prometheus_CSV("http://localhost:9090",q_range_dsn, "2025-06-01T00:00:00Z", "2025-11-01T00:00:00Z","5s")
    df = df.select(["Time","target_round_trip_seconds","dish_name","station_name","target_id","target_name"])

    df.sink_parquet(OUT_FILE)
