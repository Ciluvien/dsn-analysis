#!/usr/bin/env python3

from os import path, listdir
import xml.etree.ElementTree as ET
from ...common.OpenMetric import Metric, MetricSet
from datetime import datetime, timedelta

DATA_DIR = path.abspath(path.join(path.dirname(__file__),"../../../data/"))
INPUT_DIR = path.join(DATA_DIR,"somp2b/")
OUTPUT_DIR = path.join(DATA_DIR,"openmetric/")

INTERRUPT_INTERVAL = 60*60
TIME_INCLUDED_BEFORE_RX = 10


def get_datetime(string: str) -> datetime | None:
    try:
        time = datetime.strptime(string+"UTC+0000", "%Y%m%d-%H%M%S%Z%z")
    except Exception:
        print(f"Failed to parse time string: {string}")
        time = None
    return time

def get_passes(tree):
    passes = []
    first = None
    last = None
    for elem in tree.iter('RX'):
        elem_time_string = elem.get("DateTimeUTC")
        if elem_time_string is None:
            continue
        elem_time = get_datetime(elem_time_string)
        if elem_time is None:
            continue

        if first is None or last is None:
            first = elem_time
            last = elem_time
            continue
        if (elem_time - last).total_seconds() > INTERRUPT_INTERVAL:
            passes.append((first - timedelta(seconds = TIME_INCLUDED_BEFORE_RX), last))
            first = elem_time
        last = elem_time
    return passes


ms = MetricSet()
for f in listdir(INPUT_DIR):
    f_path = path.join(INPUT_DIR, f)
    with open(f_path, "r") as somp_file:
        try:
            tree = ET.parse(f_path).getroot()
            if tree is None:
                continue
        except Exception:
            print(f"Failed to read {f_path}")
            continue

    passes = get_passes(tree)

    pass_total_bytes_rx = 0
    pass_total_bytes_tx = 0
    current_pass = passes.pop(0)
    for elem in tree.iter():
        elem_type = elem.tag
        elem_time_string = elem.get("DateTimeUTC")
        if elem_time_string is None:
            continue
        elem_time = get_datetime(elem_time_string)
        if elem_time is None:
            continue
        elem_len = int(str(elem.get("length"))) if elem.get("length") else 0
        elem_text = elem.text
        elem_code = elem_text[1:6] if elem_text and len(elem_text) >= 5 else "None"
        if elem_time is None or elem_len is None:
            continue

        # Reset pass
        if elem_time > current_pass[1]:
            current_pass = passes.pop(0) if passes else current_pass
            pass_total_bytes_rx = 0
            pass_total_bytes_tx = 0

        if elem_time >= current_pass[0]:
            if elem_type == "RX":
                pass_total_bytes_rx += elem_len
            if elem_type == "TX":
                pass_total_bytes_tx += elem_len

        direction = "down" if elem_type == "RX" else "up"
        labels = {
            "data_source": "SOMP2B",
            "signal_direction": direction,
            "message_code": elem_code
        }

        if elem_time >= current_pass[0] and elem_time <= current_pass[1]:
            pass_total_bytes = pass_total_bytes_rx if elem_type == "RX" else pass_total_bytes_tx
            metric = Metric(name="pass_transmitted_bytes_total", value=pass_total_bytes, labels=labels, mtype="counter", timestamp=int(elem_time.timestamp()))
            ms.insert(metric)

        metric = Metric(name="transmitted", value=elem_len, labels=labels, mtype="gauge", munit="bytes", timestamp=int(elem_time.timestamp()))
        ms.insert(metric)

out_file_path = path.join(OUTPUT_DIR, "somp2b.om")
with open(out_file_path, "w") as out_file:
    out_file.write(str(ms))
