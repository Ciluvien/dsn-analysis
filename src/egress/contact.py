#!/usr/bin/env python3

import polars as pl
import json
import argparse
from enum import Enum
from extract import query_prometheus_CSV
import logging

logger = logging.getLogger(__name__)

# Distance light travels in one second in km
LIGHT_SECOND = 299792.458

# URL of the running Prometheus instance
PROMETHEUS_URL = "http://localhost:9090"

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


def get_contacts(df: pl.LazyFrame):
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
    interval = df.filter(pl.col("diff") > 0).select("diff").min().collect().item()
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


def format_contacts(df: pl.LazyFrame, form: Format, start_time_string: str | None) -> str:
    if start_time_string:
        start_time = pl.Series([start_time_string]).str.to_datetime().item()
    else:
        start_time = None

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
        result = df.collect().write_csv()
    if form == Format.HDTN:
        df = df.drop("range_km")
        result = json.dumps(json.loads(df.collect().write_json()), indent=4)

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
            [row_to_string(row, "contact") for row in df.collect().iter_rows(named=True)]
        ) + "\n" + "\n".join(
            [row_to_string(row, "range") for row in df.collect().iter_rows(named=True)]
        )
        return contacts

    return result


def contact_query(start, end, step) -> pl.LazyFrame:
    """Querys Prometheus to acquire data for the given time period"""
    # TODO: dynamic queries
    q_data_rate = r'signal_data_rate_b_per_s{station_name=~".*", target_name=~"JWST", dish_activity="Spacecraft Telemetry, Tracking, and Command",signal_activity="true"}'
    q_range_dsn = r'target_range_km{data_source=~"DSN Now", station_name=~".*", target_name=~"JWST"}'
    q_range_spice =   r'target_range_km{data_source=~"SPICE", station_name=~".*", target_id=~"-170"}'

    df_data_rate = query_prometheus_CSV(PROMETHEUS_URL, q_data_rate, start, end, step)
    df_range_dsn = query_prometheus_CSV(PROMETHEUS_URL, q_range_dsn, start, end, step)
    df_range_spice = query_prometheus_CSV(PROMETHEUS_URL, q_range_spice, start, end, step)

    df_range_dsn = (df_range_dsn
                    .rename({"target_range_km": 'Value #DSN Distance'})
                    .select(["Time", "dish_name", "station_name", "target_id", "target_name", "Value #DSN Distance"])
                    .with_columns(pl.from_epoch(pl.col("Time").floordiv(5).mul(5), time_unit="s").dt.replace_time_zone("UTC")))
    df_range_spice = (df_range_spice
                      .rename({"target_range_km": 'Value #SPICE Distance'})
                      .select(["Time", "target_id", "station_name", "Value #SPICE Distance"])
                      .with_columns(pl.from_epoch(pl.col("Time").floordiv(5).mul(5), time_unit="s").dt.replace_time_zone("UTC")))
    df_data_rate = (df_data_rate
                    .rename({"signal_data_rate_b_per_s": 'Value #Data Rate'})
                    .select(["Time", "dish_name", "signal_direction", "signal_band", "station_name", "target_id", "target_name", "Value #Data Rate"])
                    .with_columns(pl.from_epoch(pl.col("Time").floordiv(5).mul(5), time_unit="s").dt.replace_time_zone("UTC")))

    # print(df_range_dsn.head(10).collect())
    # print(df_range_spice.head(10).collect())
    # print(df_data_rate.head(10).collect())

    df_range = df_range_dsn.join(
        other = df_range_spice,
        on = ['Time','station_name','target_id'],
        how = "full",
        coalesce = True
    )

    # print(df_range.head(50).collect())
    # print(df_range.tail(50).collect())

    df = df_range.join(
        other = df_data_rate,
        on = ['Time','dish_name', 'station_name','target_id','target_name'],
        how = "full",
        coalesce = True
    ).filter(pl.col("Value #Data Rate").is_null().not_())

    return df



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse the Grafana data for contacts"
    )
    parser.add_argument("-i", "--input", help="Path to contacts CSV exported from Grafana")
    parser.add_argument("-o","--output", help="Path to output file; printing to console otherwise")
    parser.add_argument("-s","--start_time", help="Start time")
    parser.add_argument("-e","--end_time", help="End time")
    parser.add_argument("-r", "--relative_time", action="store_true", help="Output real timestamps or duration relative to --start_time")
    parser.add_argument("--step", help="Step size", default="5s")
    parser.add_argument("-f","--format",help="DTN contact plan format (RAW, HDTN, ION)")
    parser.add_argument("-l", "--log", help="Loglevel", default="info")
    args = parser.parse_args()

    if args.log:
        numeric_level = getattr(logging, args.log.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: %s' % args.log)
    else:
        numeric_level = logging.INFO
    logging.basicConfig(level=numeric_level)

    # Configure polars for easier debugging
    pl.Config.set_tbl_width_chars(110)
    pl.Config.set_tbl_rows(50)
    pl.Config.set_tbl_cols(-1)

    # Parse args
    if not args.input and not (args.start_time and args.end_time):
        exit(1)

    if args.format:
        plan_format = Format[args.format]
    else:
        plan_format = Format.RAW


    if args.input:
        # Read CSV exported from Grafana
        df = pl.scan_csv(args.input, schema=SCHEMA)
    else:
        # Query Prometheus for specified parameters
        df = contact_query(args.start_time, args.end_time, args.step)

    # Find contacts and build plan
    contacts = get_contacts(df)
    if args.relative_time:
        plan = format_contacts(contacts, plan_format, args.start_time)
    else:
        plan = format_contacts(contacts, plan_format, None)


    # Write output
    if args.output:
        with open(args.output, "w") as out_file:
            out_file.write(plan)
    else:
        print(plan)
