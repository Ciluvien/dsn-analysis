#!/usr/bin/env python3

import argparse
import json
import tempfile
import zipfile
import logging
from random import randint
from time import time
from os import path, listdir, mkdir
from shutil import rmtree
from .rewrite import xml_path_to_dict
from ...common.OpenMetric import Metric, MetricSet

logger = logging.getLogger(__name__)

def get_num(dic, key):
    val = dic[key]
    try:
        float(val)
    except ValueError:
        return "NaN"
    return val

def get_bool(dic, key):
    val = dic[key]
    if val == "false":
        return 0
    return 1


def dict_to_openmetrics(dic) -> list[Metric]:
    metrics = []
    dsn = dic.get("dsn", None)
    if not dsn:
        logger.exception(f"XML file does not contain dsn element: {dic}")
        return []
    stations = dsn.get("station", [])
    timestamp = dsn.get("timestamp", None)
    timestamp = timestamp[0:-3] if timestamp else None
    for station in stations:
        station_name = station["@name"]
        dishes = station.get("dish", [])
        if isinstance(dishes, dict):
            dishes=[dishes]
        for dish in dishes:
            dish_labels = {
                "data_source" : "DSN Now",
                "station_name" : station_name,
                "dish_name" : dish["@name"],
                "dish_activity" : dish["@activity"]
            }
            metrics.append(Metric("dish_azimuth_angle", get_num(dish,"@azimuthAngle"), labels=dish_labels, timestamp=timestamp, mtype="gauge", munit="degrees"))
            metrics.append(Metric("dish_elevation_angle", get_num(dish,"@elevationAngle"), labels=dish_labels, timestamp=timestamp, mtype="gauge", munit="degrees"))
            metrics.append(Metric("dish_wind_speed", get_num(dish, "@windSpeed"), labels=dish_labels, timestamp=timestamp, mtype="gauge", munit="km_per_h"))
            metrics.append(Metric("dish_mspa_bool", get_bool(dish,"@isMSPA"), labels=dish_labels, timestamp=timestamp, mtype="gauge"))
            metrics.append(Metric("dish_array_bool", get_bool(dish,"@isArray"), labels=dish_labels, timestamp=timestamp, mtype="gauge"))
            metrics.append(Metric("dish_ddor_bool", get_bool(dish,"@isDDOR"), labels=dish_labels, timestamp=timestamp, mtype="gauge"))

            targets = dish.get("target", [])
            if isinstance(targets, dict):
                targets = [targets]
            for target in targets:
                target_labels = {
                    "target_name" : target["@name"],
                    "target_id" : f'-{get_num(target, "@id")}'
                }
                target_labels.update(dish_labels)

                metrics.append(Metric("target_round_trip", get_num(target,"@rtlt"), labels=target_labels, timestamp=timestamp, mtype="gauge", munit="seconds"))

                target_up_labels = {
                    "target_direction" : "up"
                }
                target_up_labels.update(target_labels)
                metrics.append(Metric("target_range", get_num(target,"@uplegRange"), labels=target_up_labels, timestamp=timestamp, mtype="gauge", munit="km"))

                target_down_labels = {
                    "target_direction" : "down"
                }
                target_down_labels.update(target_labels)
                metrics.append(Metric("target_range", get_num(target,"@downlegRange"), labels=target_down_labels, timestamp=timestamp, mtype="gauge", munit="km"))


                upsignals = target.get("upSignal", [])
                if isinstance(upsignals, dict):
                    upsignals = [upsignals]
                for signal in upsignals:
                    signal_labels = {
                        "signal_direction" : "up",
                        "signal_activity" : signal["@active"],
                        "signal_type" : signal["@signalType"],
                        "signal_band" : signal["@band"]
                    }
                    signal_labels.update(target_labels)

                    # Convert MHz to Hz
                    frequency = get_num(signal,"@frequency")
                    if frequency.isnumeric():
                        frequency = int(frequency) * 1000000

                    metrics.append(Metric("signal_data_rate", get_num(signal,"@dataRate"), labels=signal_labels, timestamp=timestamp, mtype="gauge", munit="b_per_s"))
                    metrics.append(Metric("signal_frequency", frequency, labels=signal_labels, timestamp=timestamp, mtype="gauge", munit="Hz"))
                    metrics.append(Metric("signal_power_sent", get_num(signal, "@power"), labels=signal_labels, timestamp=timestamp, mtype="gauge", munit="kW"))


                downsignals = target.get("downSignal", [])
                if isinstance(downsignals, dict):
                    downsignals = [downsignals]
                index = 0
                for signal in downsignals:
                    signal_labels = {
                        "signal_direction" : "down",
                        "signal_activity" : signal["@active"],
                        "signal_type" : signal["@signalType"],
                        "signal_band" : signal["@band"],
                        "signal_index": str(index)
                    }
                    signal_labels.update(target_labels)
                    metrics.append(Metric("signal_data_rate", get_num(signal,"@dataRate"), labels=signal_labels, timestamp=timestamp, mtype="gauge", munit="b_per_s"))
                    metrics.append(Metric("signal_frequency", get_num(signal,"@frequency"), labels=signal_labels, timestamp=timestamp, mtype="gauge", munit="Hz"))
                    metrics.append(Metric("signal_power_received", get_num(signal,"@power"), labels=signal_labels, timestamp=timestamp, mtype="gauge", munit="dBm"))
                    index += 1


    return metrics

def process_batch(directory: str, is_xml: bool) -> MetricSet:
    if directory[-1] != "/":
        directory += "/"
    files = [f"{directory}{f}" for f in listdir(directory) if path.isfile(f"{directory}{f}")]

    result = MetricSet()
    for f in files:
        if is_xml:
            dic = xml_path_to_dict(f)
        else:
            with open(f) as json_file:
                dic = json.loads(json_file.read())

        if dic:
            try:
                om = dict_to_openmetrics(dic)
            except Exception:
                logger.error(f"Failed to parse {dic}", exc_info=True)
            else:
                for metric in om:
                    result.insert(metric)

    return result

def openmetrify(is_batch: bool, is_xml: bool, input_path: str, output_path: str):
    # Process batches separately
    if is_batch:
        file_name = path.basename(input_path)
        if path.isdir(input_path):
            start = time()
            result = process_batch(input_path, is_xml)
            logger.info(f"Processing {file_name} took {time()-start}")
        elif path.isfile(input_path) and ".zip" in input_path:
            temp_dir_base = tempfile.gettempdir() + "/openmetrify_extracted"

            # create random temp directory without conflict
            suffix=randint(0,999999)
            temp_dir = f"{temp_dir_base}{suffix:06d}"
            while path.isdir(temp_dir):
                suffix=randint(0,999999)
                temp_dir=f"{temp_dir_base}{suffix:06d}"
            mkdir(temp_dir)

            # Extract archive
            start = time()
            with zipfile.ZipFile(input_path, "r") as zipf:
                zipf.extractall(temp_dir)
            logger.info(f"Extracting {file_name} took {time()-start}")

            # Process contents
            start = time()
            result = process_batch(temp_dir, is_xml)
            logger.info(f"Processing {file_name} took {time()-start}")
            if path.isdir(temp_dir):
                rmtree(temp_dir)
        else:
            logger.info(f"Could not process input path {input_path}")
            exit(1)

        start = time()
        res_string = str(result)
        logger.info(f"Creating output string for {file_name} took {time()-start}")
        start = time()
        with open(output_path, "w") as om_file:
            om_file.write(res_string)
        logger.info(f"Writing output for {file_name} took {time()-start}")

    else: # Single file processing mode
        if is_xml:
            dic = xml_path_to_dict(input_path)
        else:
            with open(input_path) as json_file:
                dic = json.loads(json_file.read())

        try:
            om = dict_to_openmetrics(dic)
        except Exception:
            logger.error(f"Failed to parse {dic}", exc_info=True)
        else:
            ms = MetricSet()
            for metric in om:
                ms.insert(metric)
            with open(output_path, "w") as om_file:
                om_file.write(str(ms))



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert dsn json to OpenMetrics"
    )
    parser.add_argument("-b","--batch", action="store_true", help="Treat input as collection and output to single file")
    parser.add_argument("-x","--xml", action="store_true", help="Work directly on DSN XML files instead of converted json")
    parser.add_argument("-l","--log",help="Loglevel")
    parser.add_argument("input")
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


    openmetrify(is_batch=args.batch, is_xml=args.xml, input_path=args.input, output_path=args.output)
