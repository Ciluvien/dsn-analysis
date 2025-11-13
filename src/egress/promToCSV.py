#!/usr/bin/env python3

import logging
import polars as pl
from collections import defaultdict

logger = logging.getLogger(__name__)

def matrixToCSV(matrix: dict) -> pl.LazyFrame:
    if matrix["resultType"] != "matrix":
        logger.error("Result is not a matrix")
        exit(1)

    result = pl.LazyFrame({})
    for metric in matrix["result"]:
        metric_name = metric["metric"]["__name__"]
        labels = [(k,v) for k, v in metric["metric"].items() if k != "__name__"]

        metric_as_table = defaultdict(list)
        for vals in metric["values"]:
            metric_as_table["Time"].append(int(vals[0]))
            metric_as_table[metric_name].append(float(vals[1]))

        new_table_size = len(metric_as_table["Time"])
        for label in labels:
            metric_as_table[label[0]].extend([label[1]]*new_table_size)

        df = pl.LazyFrame(metric_as_table)

        # Cast Int to Float for value columns where both can appear
        # if metric_name in result.collect_schema().names():
        #     df_type = df.select(metric_name).collect_schema().dtypes()[0]
        #     result_type = result.select(metric_name).collect_schema().dtypes()[0]
        #     if df_type.is_integer() and result_type.is_float():
        #         df = df.cast({metric_name: result_type})
        #     elif df_type.is_float() and result_type.is_integer():
        #         result = result.cast({metric_name: df_type})

        result = pl.concat([result, df], how="diagonal_relaxed")

    return result
