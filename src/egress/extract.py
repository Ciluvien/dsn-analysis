#!/usr/bin/env python3
import argparse
import logging
import requests
import uuid
import re
import json
import datetime as dt
import polars as pl
from promToCSV import matrixToCSV
from os import path, mkdir

MAX_INTERVAL_COUNT = 1024

logger = logging.getLogger(__name__)

def prepare_query_string(query) -> str:
    """Add quotation marks around label values, in case the console ate them"""
    return re.sub(r'(\w+)=[~]["]([^,}]+)["]', r'\1=~"\2"', query)

def split_time_range(start, end, count) -> list[str]:
    start = dt.datetime.fromisoformat(start)
    end = dt.datetime.fromisoformat(end)

    splits = [start + i*(end-start)/count for i in range(count+1)]

    intervals = []
    for i in range(len(splits) - 1):
        intervals.append([splits[i].isoformat(), splits[i+1].isoformat()])

    logger.debug(f"New time intervals: {intervals}")
    return intervals

def query_prometheus(
        prometheus: str,
        query: str,
        start: str,
        end: str,
        step: str) -> dict | requests.Response:

    params = {
        "query": query,
        "start": start,
        "end": end,
        "step": step
    }

    logger.debug(f"Params: {params}")
    response = requests.get(url=f"{prometheus}/api/v1/query_range", params=params)

    match response.status_code:
        case 200:
            logger.debug(f"Query for {query} between {start} and {end} succesful")
            return response.json()['data']
        case 404:
            logger.error(f"No Prometheus instance at {prometheus}")
        case _:
            logger.debug(f"Request warning: {response.status_code}, {response.text}")

    return response


def is_step_size_error(response: requests.Response) -> bool:
    return "exceeded maximum resolution" in response.json()["error"]


def query_prometheus_split(
        prometheus: str,
        query: str,
        start: str,
        end: str,
        step: str) -> dict:

    logger.info(f"Querying for {query}")

    response = query_prometheus(prometheus, query, start, end, step)

    if isinstance(response, dict):
        return {0: response}

    interval_count = 2
    step_size_error = is_step_size_error(response)
    response_dict = {}
    while step_size_error and interval_count < MAX_INTERVAL_COUNT:
        step_size_error = False
        logger.info(f"Step size requires multiple queries, trying {interval_count} intervals")

        for index, interval in enumerate(split_time_range(start, end, interval_count)):
            response = query_prometheus(prometheus, query, interval[0], interval[1], step)

            if isinstance(response, dict):
                response_dict[index] = response
            else:
                step_size_error = is_step_size_error(response)
                if step_size_error:
                    logger.debug("Exceeding query limits")
                    response_dict.clear()
                    break

        interval_count *= 2

    if step_size_error and interval_count >= MAX_INTERVAL_COUNT:
        logger.error("You should probably increase the step size")
        exit(1)

    return response_dict

def query_prometheus_CSV(
        prometheus: str,
        query: str,
        start: str,
        end: str,
        step: str) -> pl.LazyFrame:

    response_dict = query_prometheus_split(prometheus, query, start, end, step)

    temp_dir = f"/tmp/{uuid.uuid4()}"
    try:
        mkdir(temp_dir)
    except FileExistsError:
        logger.info("Temp dir already exists")

    for index, metric in enumerate(response_dict.values()):
        temp_file_path = path.join(temp_dir,f"{index}.parquet")
        matrixToCSV(metric).sink_parquet(temp_file_path)

    df = pl.scan_parquet(f"{temp_dir}/*")
    if df.select(pl.len()).collect().item() == 0:
        logger.warning(f"Dataframe for query {query} is empty")

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract data from Prometheus and convert it to CSV")
    parser.add_argument("prometheus",help="URL of the Prometheus instance")
    parser.add_argument("query", help="Prometheus query string")
    parser.add_argument("start", help="Start time")
    parser.add_argument("end", help="End time")
    parser.add_argument("-o", "--output", help="Path to output file")
    parser.add_argument("-s", "--step", help="Step size", default="5s")
    parser.add_argument("-c", "--csv", action="store_true" , help="Convert to CSV")
    parser.add_argument("-l", "--log", help="Loglevel", default="info")
    args = parser.parse_args()

    if args.log:
        numeric_level = getattr(logging, args.log.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: %s' % args.log)
    else:
        numeric_level = logging.INFO
    logging.basicConfig(level=numeric_level)

    if args.csv:
        csv = query_prometheus_CSV(args.prometheus, prepare_query_string(args.query), args.start, args.end, args.step)
        if args.output:
            csv.sink_csv(args.output)
        else:
            print(csv)
    else:
        response = query_prometheus_split(args.prometheus, prepare_query_string(args.query), args.start, args.end, args.step)
        if args.output:
            with open(args.output, "w") as f:
                json.dump(response, f)
        else:
            print(response)
