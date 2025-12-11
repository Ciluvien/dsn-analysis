#!/usr/bin/env python3

import argparse
import logging
import tempfile
import zipfile
import math
import polars as pl
from dataclasses import dataclass
from os import listdir, path, getcwd
from .rewrite import xml_path_to_dict

WORKING_DIR = getcwd()
logger = logging.getLogger(__name__)

POLARS_SCHEMA = {
    "timestamp": pl.Int64,
    "station_name": pl.String,
    "dish_name": pl.String,
    "dish_activity": pl.String,
    "dish_azimuth_angle_degrees": pl.Float64,
    "dish_elevation_angel_degrees": pl.Float64,
    "dish_wind_speed_km_per_h": pl.Float64,
    "dish_mspa_bool": pl.Boolean,
    "dish_array_bool": pl.Boolean,
    "dish_ddor_bool": pl.Boolean,
    "target_name": pl.String,
    "target_id": pl.Int32,
    "target_round_trip_seconds": pl.Float64,
    "target_upleg_range_km": pl.Int64,
    "target_downleg_range_km": pl.Int64,
    "signal_direction": pl.String,
    "signal_activity": pl.String,
    "signal_type": pl.String,
    "signal_band": pl.String,
    "signal_data_rate_b_per_s": pl.Int64,
    "signal_frequency_Hz": pl.Int64,
    "signal_power_received_dBm": pl.Float64,
    "signal_power_sent_kW": pl.Float64,
}

@dataclass
class dsn_file:
    timestamp: int
    stations: list[dsn_station]

    def to_column_dict_rows(self):
        cols: dict[str, list[object]] = {k: [] for k in POLARS_SCHEMA.keys()}

        for station in self.stations:
            for dish in station.dishes:
                for target in dish.targets:
                    for sig in target.signals:
                        cols["timestamp"].append(self.timestamp)
                        cols["station_name"].append(station.station_name)
                        cols["dish_name"].append(dish.dish_name)
                        cols["dish_activity"].append(dish.dish_activity)
                        cols["dish_azimuth_angle_degrees"].append(dish.dish_azimuth_angle_degrees)
                        cols["dish_elevation_angel_degrees"].append(dish.dish_elevation_angel_degrees)
                        cols["dish_wind_speed_km_per_h"].append(dish.dish_wind_speed_km_per_h)
                        cols["dish_mspa_bool"].append(dish.dish_mspa_bool)
                        cols["dish_array_bool"].append(dish.dish_array_bool)
                        cols["dish_ddor_bool"].append(dish.dish_ddor_bool)
                        cols["target_name"].append(target.target_name)
                        cols["target_id"].append(target.target_id)
                        cols["target_round_trip_seconds"].append(target.target_round_trip_seconds)
                        cols["target_upleg_range_km"].append(target.target_upleg_range_km)
                        cols["target_downleg_range_km"].append(target.target_downleg_range_km)
                        cols["signal_direction"].append(sig.signal_direction)
                        cols["signal_activity"].append(sig.signal_activity)
                        cols["signal_type"].append(sig.signal_type)
                        cols["signal_band"].append(sig.signal_band)
                        cols["signal_data_rate_b_per_s"].append(sig.signal_data_rate_b_per_s)
                        cols["signal_frequency_Hz"].append(sig.signal_frequency_Hz)
                        cols["signal_power_received_dBm"].append(sig.signal_power_received_dBm)
                        cols["signal_power_sent_kW"].append(sig.signal_power_sent_kW)
        return cols

@dataclass
class dsn_station:
    station_name: str
    dishes: list[dsn_dish]

@dataclass
class dsn_dish:
    dish_name: str
    dish_activity: str
    dish_azimuth_angle_degrees: float
    dish_elevation_angel_degrees: float
    dish_wind_speed_km_per_h: float
    dish_mspa_bool: bool
    dish_array_bool: bool
    dish_ddor_bool: bool
    targets: list[dsn_target]

@dataclass
class dsn_target:
    target_name: str
    target_id: int | None
    target_round_trip_seconds: float | None
    target_upleg_range_km: int | None
    target_downleg_range_km: int| None
    signals: list[dsn_signal]

@dataclass
class dsn_signal:
    signal_direction: str
    signal_activity: str
    signal_type: str
    signal_band: str
    signal_data_rate_b_per_s: int| None
    signal_frequency_Hz: int| None
    signal_power_received_dBm: float | None
    signal_power_sent_kW: float | None


def get_num(dic, key) -> float:
    val = dic[key]
    try:
        return float(val)
    except ValueError:
        return float("nan")

def get_bool(dic, key):
    val = dic[key]
    if val == "false":
        return False
    return True


def parse_dsn(in_file) -> dsn_file | None:
    dic = xml_path_to_dict(in_file)

    if not dic:
        return None

    dsn = dic.get("dsn", None)
    if not dsn:
        logger.exception(f"XML file does not contain dsn element: {dic}")
        return None

    stations = dsn.get("station", None)
    if not stations:
        logger.exception(f"XML file does not contain stations: {dic}")
        return None

    timestamp = dsn.get("timestamp", None)
    if not timestamp:
        logger.exception(f"XML file does not contain timestamp: {dic}")
        return None
    timestamp = int(timestamp[0:-3])

    dsn_stations: list[dsn_station] = []
    for station in stations:
        station_name = station["@name"]
        dishes = station.get("dish", [])
        if isinstance(dishes, dict):
            dishes=[dishes]

        dsn_dishes: list[dsn_dish] = []
        for dish in dishes:
            dish_name = dish["@name"]
            dish_activity = dish["@activity"]
            dish_azimuth_angle = get_num(dish,"@azimuthAngle")
            dish_elevation_angle = get_num(dish,"@elevationAngle")
            dish_wind_speed = get_num(dish, "@windSpeed")
            dish_mspa_bool = get_bool(dish,"@isMSPA")
            dish_array_bool = get_bool(dish,"@isArray")
            dish_ddor_bool = get_bool(dish,"@isDDOR")


            targets = dish.get("target", [])
            if isinstance(targets, dict):
                targets = [targets]
            dsn_targets: list[dsn_target] = []
            for target in targets:
                target_name = target["@name"]
                target_id = get_num(target, "@id")
                target_id = int(target_id * -1) if not math.isnan(target_id) else None
                target_round_trip = get_num(target, "@rtlt")
                target_upleg_range = int(get_num(target, "@uplegRange"))
                target_downleg_range = int(get_num(target, "@downlegRange"))

                dsn_signals: list[dsn_signal] = []

                upsignals = target.get("upSignal", [])
                if isinstance(upsignals, dict):
                    upsignals = [upsignals]
                for signal in upsignals:
                    signal_direction = "up"
                    signal_activity = signal["@active"]
                    signal_type = signal["@signalType"]
                    signal_band = signal["@band"]

                    signal_data_rate = get_num(signal, "@dataRate")
                    signal_data_rate = int(signal_data_rate) if not math.isnan(signal_data_rate) else None
                    # Convert MHz to Hz
                    signal_frequency = get_num(signal,"@frequency")
                    signal_frequency = int(signal_frequency) * 1000000 if not math.isnan(signal_frequency) else None
                    signal_power_sent = get_num(signal, "@power")
                    signal_power_received = None


                    dsn_signals.append(
                        dsn_signal(
                            signal_direction,
                            signal_activity,
                            signal_type,
                            signal_band,
                            signal_data_rate,
                            signal_frequency,
                            signal_power_received,
                            signal_power_sent,
                        )
                    )

                downsignals = target.get("downSignal", [])
                if isinstance(downsignals, dict):
                    downsignals = [downsignals]
                for signal in downsignals:
                    signal_direction = "down"
                    signal_activity = signal["@active"]
                    signal_type = signal["@signalType"]
                    signal_band = signal["@band"]

                    signal_data_rate = get_num(signal, "@dataRate")
                    signal_data_rate = int(signal_data_rate) if not math.isnan(signal_data_rate) else None
                    signal_frequency = get_num(signal,"@frequency")
                    signal_frequency = int(signal_frequency) if not math.isnan(signal_frequency) else None
                    signal_power_sent = None
                    signal_power_received = get_num(signal, "@power")
                    signal_power_received = signal_power_received if not math.isnan(signal_power_received) else None

                    dsn_signals.append(
                        dsn_signal(
                            signal_direction,
                            signal_activity,
                            signal_type,
                            signal_band,
                            signal_data_rate,
                            signal_frequency,
                            signal_power_received,
                            signal_power_sent,
                        )
                    )

                dsn_targets.append(
                    dsn_target(
                        target_name,
                        target_id,
                        target_round_trip,
                        target_upleg_range,
                        target_downleg_range,
                        dsn_signals
                    )
                )

            dsn_dishes.append(
                dsn_dish(dish_name,
                         dish_activity,
                         dish_azimuth_angle,
                         dish_elevation_angle,
                         dish_wind_speed,
                         dish_mspa_bool,
                         dish_array_bool,
                         dish_ddor_bool,
                         dsn_targets
                         )
            )
        dsn_stations.append(dsn_station(station_name, dsn_dishes))
    return dsn_file(timestamp, dsn_stations)


def dsn_dir_to_parquet(in_dir: str, out_file: str):
    dir_path = path.join(in_dir)
    with tempfile.TemporaryDirectory(dir=WORKING_DIR) as tmp_dir:
        for f in listdir(dir_path):
            file_path = path.join(dir_path, f)
            dsn_file = parse_dsn(file_path)
            if not dsn_file:
                logger.exception(f"Failed to parse: {file_path}")
                continue
            tmp_file = path.join(tmp_dir,f"{f}.parquet")
            df_tmp = pl.DataFrame(dsn_file.to_column_dict_rows(), POLARS_SCHEMA)
            df_tmp.write_parquet(tmp_file)
        df = pl.scan_parquet(source = tmp_dir, schema = POLARS_SCHEMA)
        df.sink_parquet(out_file)


def dsn_to_parquet(in_file: str, out_file: str, is_zip: bool):
    if is_zip:
        with tempfile.TemporaryDirectory(dir=WORKING_DIR) as tmp_dir:
            with zipfile.ZipFile(in_file, "r") as zipf:
                zipf.extractall(tmp_dir)
            dsn_dir_to_parquet(tmp_dir, out_file)
    else:
        dsn_dir_to_parquet(in_file, out_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="convert DSN Now XML files to parquet files"
    )
    parser.add_argument("-l","--log",help="loglevel")
    parser.add_argument("-z","--zip", action="store_true", help="treat input as a zip compressed archive of DSN Now XML files")
    parser.add_argument("input", help="directory containing DSN Now XML files")
    parser.add_argument("output")
    args = parser.parse_args()

    # Prepare logging
    if args.log:
        numeric_level = getattr(logging, args.log.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: %s' % args.log)
    else:
        numeric_level = logging.INFO
    logging.basicConfig(level=numeric_level)

    dsn_to_parquet(args.input, args.output, args.zip)
