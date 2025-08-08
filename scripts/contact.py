#!/usr/bin/env python3

import polars as pl
from os import path
import json
import argparse
from enum import Enum

# Distance light travels in one second in km
LIGHT_SECOND = 299792.458

SCHEMA = {
    'Time': pl.Datetime,
    'dish_name': pl.String,
    'signal_band': pl.String,
    'signal_direction': pl.String,
    'station_name': pl.String,
    'target_id': pl.Int64,
    'target_name': pl.String,
    'Value #Data Rate': pl.Float64,
    'Value #DSN Distance': pl.Int64,
    'Value #SPICE Distance': pl.Int64,
}

class Format(Enum):
    RAW = 0
    HDTN = 1
    ION = 2


def get_contacts(df: pl.DataFrame):
    # Add column holding time difference between consecutive samples
    df = df.sort(
        pl.col("dish_name"),
        pl.col("target_name"),
        pl.col("signal_direction"),
        pl.col("signal_band"),
        pl.col("Time")
    ).with_columns(
        (pl.col("Time").diff().fill_null(pl.duration(minutes=0))).alias("diff")
    )

    # Find minimum interval
    interval = df.filter(pl.col("diff") > 0).min().select("diff").item()
    # Assign a number to each contact
    df = df.with_columns(
        pl.when(
            (pl.col("diff") == interval) | (pl.col("diff") == pl.duration(minutes=0))
        ).then(0)\
         .otherwise(10)\
         .cum_sum().alias("contact"))

    # Split contacts when range change is larger than one light second
    df = df.with_columns(
        pl.col("Value #DSN Distance").diff().abs().cum_sum().over("contact").alias("Cum DSN"),
        pl.col("Value #SPICE Distance").diff().abs().cum_sum().over("contact").alias("Cum SPICE")
    ).with_columns(
        pl.when(pl.col("Cum SPICE").is_not_null()).then(
            pl.when(
                (pl.col("Cum SPICE") > LIGHT_SECOND)
            ).then(
                (pl.col("contact") + pl.col("Cum SPICE").floordiv(LIGHT_SECOND).cast(pl.Int64)).alias("contact")
            ).otherwise(
                pl.col("contact")
            )
        ).otherwise(
            pl.when(
                (pl.col("Cum DSN") > LIGHT_SECOND)
            ).then(
                (pl.col("contact") + pl.col("Cum DSN").floordiv(LIGHT_SECOND).cast(pl.Int64)).alias("contact")
            ).otherwise(
                pl.col("contact")
            )
        )
    )

    # Group contacts and calculate mean distances and data rate
    df = df.group_by(
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


def format_contacts(df: pl.DataFrame, form: Format, start_time: None | pl.Datetime) -> str:
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

    df = df.with_columns(
        (pl.col("range_km") / LIGHT_SECOND).round().cast(pl.UInt64).alias("owlt"),
    )

    result = ""
    if form == Format.RAW:
        result = df.write_csv()
    if form == Format.HDTN:
        df = df.drop("range_km")
        result = json.dumps(json.loads(df.write_json()), indent=4)

    elif form == Format.ION:
        def row_to_string(row: dict, mode: str) -> str:
            if mode == "contact":
                val = str(int(row["rateBitsPerSec"] / 8))
            elif mode == "range":
                val = str(row["owlt"])
            else:
                exit(1)
            if start_time:
                return " ".join(
                    (
                        f"a {mode}",
                        "+"+str(row["startTime"]),
                        "+"+str(row["endTime"]),
                        str(row["source"]),
                        str(row["dest"]),
                        val)
                    )
            else:
                return " ".join(
                    (
                        f"a {mode}",
                        str(row["startTime"]),
                        str(row["endTime"]),
                        str(row["source"]),
                        str(row["dest"]),
                        val
                    )
                )


        contacts = "\n".join(
            [row_to_string(row, "contact") for row in df.iter_rows(named=True)]
        ) + "\n" + "\n".join(
            [row_to_string(row, "range") for row in df.iter_rows(named=True)]
        )
        return contacts

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse the Grafana data for contacts"
    )
    parser.add_argument("input", help="path to contacts CSV exported from Grafana")
    parser.add_argument("-o","--output", help="path to output file; printing to console otherwise")
    parser.add_argument("-s","--start_time", help="start time for relative contact plans")
    parser.add_argument("-f","--format",help="DTN contact plan format (RAW, HDTN, ION)")
    args = parser.parse_args()

    # Parse args
    if args.format:
        plan_format = Format[args.format]
    else:
        plan_format = Format.RAW

    if args.start_time:
        start_time = pl.Series([args.start_time]).str.to_datetime().item()
    else:
        start_time = None

    # Read CSV
    df = pl.read_csv(args.input, schema=SCHEMA)

    # Configure polars for easier debugging
    pl.Config.set_tbl_width_chars(110)
    pl.Config.set_tbl_rows(100)
    pl.Config.set_tbl_cols(-1)

    # Find contacts and build plan
    contacts = get_contacts(df)
    plan = format_contacts(contacts, plan_format, start_time)

    # Write output
    if args.output:
        with open(args.output, "w") as out_file:
            out_file.write(plan)
    else:
        print(plan)
