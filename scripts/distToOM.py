#!/usr/bin/env python3

import polars as pl
from os import path
from OpenMetric import Metric, MetricSet
import argparse

CSV_PATH = path.join("../data/distances.csv")
OUT_PATH = path.join("../data/distances.om")

if __name__ == "__main__":
     parser = argparse.ArgumentParser(
          description="Convert distance CSV to OpenMetrics file"
     )
     parser.add_argument("--input", help="Path to CSV", default=CSV_PATH)
     args = parser.parse_args()

     df = pl.read_csv(args.input)

     ms = MetricSet()

     for row in df.iter_rows(named=True):
          time = row["time"]
          station = row["station"]
          target = row["target"]
          distance = row["distance"]
          labels = {
               "data_source": "SPICE",
               "station_name": station,
               "target_id": target
          }
          om = Metric(name="target_range", value=distance, labels=labels, mtype="gauge", munit="km", timestamp=time)
          ms.insert(om)


     om_path = f"{args.input}.om"
     with open(om_path, "w") as om_file:
          om_file.write(str(ms))

     # Move file to openmetric folder, then execute command:
     # docker exec -it prometheus promtool tsdb create-blocks-from openmetrics --max-block-duration=7d -r /openmetric/distances14.csv.om /prometheus
