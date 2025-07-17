#!/usr/bin/env python3

import polars as pl
from os import path
import json
import argparse
from enum import Enum


class Format(Enum):
    HDTN = 1
    ION = 2

def get_contacts(df: pl.DataFrame):
    df = df.sort(
        pl.col("dish_name"),
        pl.col("target_name"),
        pl.col("signal_direction"),
        pl.col("signal_band"),
        pl.col("Time")
    ).with_columns(
        (pl.col("Time").diff().fill_null(pl.duration(minutes=0))).alias("diff")
    )


    interval = df.filter(pl.col("diff") > 0).min().select("diff").item()
    #print(f"Minimum interval is {interval}")

    df = df.with_columns(
        pl.when(
            (pl.col("diff") == interval)
            |
            (pl.col("diff") == pl.duration(seconds=0))
        )\
         .then(0)\
         .otherwise(1)\
         .cum_sum().alias("contact")
    ).group_by(
        pl.col("dish_name"),
        pl.col("target_name"),
        pl.col("signal_direction"),
        pl.col("signal_band"),
        pl.col("contact"))\
    .agg(
        [
            pl.first("Time").alias("start_time"),
            pl.last("Time").alias("end_time"),
            pl.mean("Value #Data Rate").alias("mean_data_rate"),
            pl.mean("Value #DSN Distance").alias("mean_dsn_range"),
            pl.mean("Value #SPICE Distance").alias("mean_spice_range"),
        ]
    )

    return df


def format_contacts(df: pl.DataFrame, form: Format, start_time: None | pl.Datetime):
    df = df.with_columns(
        pl.concat_str("target_name","signal_band",separator="_"),
        pl.concat_str("dish_name","signal_band", separator="_")
    ).with_columns(
        pl.when(pl.col("signal_direction") == "up").then(pl.col("dish_name")).otherwise(pl.col("target_name")).alias("source"),
        pl.when(pl.col("signal_direction") == "down").then(pl.col("dish_name")).otherwise(pl.col("target_name")).alias("dest"),
    ).with_columns(
        pl.when(pl.col("mean_spice_range").is_not_null())\
        .then(pl.col("mean_spice_range")).otherwise(pl.col("mean_dsn_range")).alias("range_km")
    )

    if start_time:
        df = df.with_columns(
            pl.col("start_time").sub(start_time).dt.total_seconds(),
            pl.col("end_time").sub(start_time).dt.total_seconds(),
        )
    else:
        df = df.with_columns(
            pl.col("start_time").dt.epoch(time_unit='s'),
            pl.col("end_time").dt.epoch(time_unit='s'),
        )

    df = df.drop_nans().select(
        "contact",
        "source",
        "dest",
        pl.col("start_time").alias("startTime"),
        pl.col("end_time").alias("endTime"),
        pl.col("mean_data_rate").cast(pl.UInt64).alias("rateBitsPerSec"),
        pl.col("range_km")
    ).sort("contact")

    result = ""
    if form == Format.HDTN:
        df = df.with_columns(
            (pl.col("range_km") / 299792.458).cast(pl.UInt64).alias("owlt"),
        ).drop("range_km")
        result = json.dumps(json.loads(df.write_json()), indent=4)

    elif form == Format.ION:
        pass

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse the Grafana data for contacts"
    )
    parser.add_argument("input", help="Path to contacts CSV exported from Grafana")
    parser.add_argument("-o","--output", help="Path to output file, prints if not given")
    parser.add_argument("-s","--start_time", help="Start time for relative contact plans")
    parser.add_argument("-f","--format",help="DTN contact plan format (HDTN, ION)")
    args = parser.parse_args()

    if args.format:
        plan_format = Format[args.format]
    else:
        plan_format = Format.HDTN

    if args.start_time:
        start_time = pl.Series([args.start_time]).str.to_datetime().item()
    else:
        start_time = None

    df = pl.read_csv(args.input, infer_schema_length=10000).drop_nulls()\
        .with_columns(
            pl.col("Time").str.to_datetime()
        )

    pl.Config.set_tbl_width_chars(110)
    pl.Config.set_tbl_rows(30)
    pl.Config.set_tbl_cols(-1)
    contacts = get_contacts(df)

    plan = format_contacts(contacts, plan_format, start_time)

    if args.output:
        with open(args.output, "w") as out_file:
            out_file.write(plan)
    else:
        print(plan)
