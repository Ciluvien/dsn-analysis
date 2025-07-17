#!/usr/bin/env python3

import polars as pl
from os import path
from OpenMetric import Metric, MetricSet
import argparse
from time import time

CSV_PATH = path.join("../data/distances-full.csv")
OUT_PATH = path.join("../data/openmetric/")

def to_metrics(df):
     ms = MetricSet()
     for row in df.iter_rows(named=True):
          timestamp = row["time"]
          station = row["station"]
          target = row["target"]
          distance = row["distance"]
          labels = {
               "data_source": "SPICE",
               "station_name": station,
               "target_id": target
          }
          om = Metric(name="target_range", value=distance, labels=labels, mtype="gauge", munit="km", timestamp=timestamp)
          ms.insert(om)
     return ms


if __name__ == "__main__":
     parser = argparse.ArgumentParser(
          description="Convert distance CSV to OpenMetrics file"
     )
     parser.add_argument("--input", help="Path to CSV", default=CSV_PATH)
     parser.add_argument("--output", help="Path to output directory", default=OUT_PATH)
     args = parser.parse_args()

     time_start = time()
     df = pl.read_csv(args.input)
     targets = pl.Series(df.select("target").unique()).to_list()
     print(f"Reading CSV for target IDs took {time() - time_start}")

     for target in targets:
          time_start = time()
          df_part = pl.scan_csv(args.input).filter(pl.col("target") == target).collect()
          target = df_part.select("target").head(1).item()
          ms = to_metrics(df_part)
          print(f"Creating MetricSet for target {target} took {time() - time_start}")
          time_start = time()
          om_path = path.join(args.output, f"{path.basename(args.input)}{target}.om")
          with open(om_path, "w") as om_file:
               om_file.write(str(ms))
               print(f"String creation and writing for {target} took {time() - time_start}")



     # Move file to openmetric folder, then execute command:
     # docker exec -it prometheus promtool tsdb create-blocks-from openmetrics --max-block-duration=7d -r /openmetric/distances14.csv.om /prometheus
