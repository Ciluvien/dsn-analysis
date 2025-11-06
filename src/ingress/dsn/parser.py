#!/usr/bin/env python3

import logging
import argparse
import time
import glob
import multiprocessing as mp
from os import path
from functools import partial
from ...common.promtool_wrapper import import_all
from .openmetrify import openmetrify

logger = logging.getLogger(__name__)

DATA_DIR = path.abspath(path.join(path.dirname(__file__),"../../../data/"))
IN_DIR = path.join(DATA_DIR,"to_be_converted/") # Path to the required DSN XML files
OUT_DIR = path.join(DATA_DIR,"openmetric/") # Path to the temporary OpenMetrics files
THREAD_COUNT = 3 # Number of concurrent threads


def process_file(f, out_dir):
    date = path.basename(f)
    om_file = path.join(out_dir, f'dsn_{date}.om')
    openmetrify(is_batch=True, is_xml=True, input_path=f, output_path=om_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse DSN XML files in batches and write the extraced data to a prometheus data folder"
    )
    parser.add_argument("-l","--log",help="Loglevel",default="info")
    parser.add_argument("-c","--convert_only", action="store_true",help="Do not import into Prometheus")
    parser.add_argument("--input",help="Folder containing DSN XML zip files", default=IN_DIR)
    parser.add_argument("--output",help="Save OpenMetrics files to this directory, requires -c",default=OUT_DIR)
    args = parser.parse_args()

    if args.log:
        numeric_level = getattr(logging, args.log.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: %s' % args.log)
    else:
        numeric_level = logging.INFO
    logging.basicConfig(level=numeric_level)

    logger.info(f"Start parsing DSN XML zip files at: {args.input}")

    if not path.isdir(args.input):
        logger.error("Input must be a directory")
        exit(1)

    if not path.isdir(args.output):
        logger.error("Output must be a directory")
        exit(1)

    if args.output != OUT_DIR and not args.convert_only:
        logger.error(f"Prometheus import only works for {OUT_DIR}. Try adding -c flag.")
        exit(1)

    # Process input files
    start_processing_time = time.time()
    files = glob.glob(path.join(args.input, '*'))
    if not files:
        logger.warning("Empty input")
        exit(1)
    with mp.Pool(THREAD_COUNT) as pool:
        func = partial(process_file, out_dir=OUT_DIR)
        pool.map(func, files)

    delta_processing_time = time.time() - start_processing_time
    logger.info(f"Converting to OpenMetrics took: {delta_processing_time} s")


    # Import OpenMetric files into Prometheus
    if not args.convert_only:
        start_import_time = time.time()
        import_all(OUT_DIR, "1d")
        delta_import_time = time.time() - start_import_time
        logger.info(f"Importing OpenMetrics into Prometheus took: {delta_import_time} s")

    logger.info(f"Finished processing {args.input}")
