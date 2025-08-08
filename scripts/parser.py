#!/usr/bin/env python3

from os import path
import subprocess
import glob
import threading
import logging
import argparse
from promtool_wrapper import import_all
import time

logger = logging.getLogger(__name__)

# Path to the required DSN XML files
in_dir = path.join("../data/to_be_converted/")
# Path to the temporary OpenMetrics files
out_dir = path.join("../data/openmetric/")

# Semaphore to limit the number of concurrent subprocesses
max_concurrent_processes = 3
semaphore = threading.Semaphore(max_concurrent_processes)

def process_file(f, date, out_dir):
    om_file = path.join(out_dir, f'dsn_{date}.om')
    with semaphore:
        logger.info(f"Start processing {f}")
        result = subprocess.run(['python', 'openmetrify.py', '-l', 'INFO', '-b', '-x', f, om_file], capture_output=True, text=True)

        if result.stdout:
            logger.info(f"openmetrify for {f} stdout:\n{result.stdout.strip()}")
        if result.stderr:
            logger.info(f"openmetrify for {f} stderr:\n{result.stderr.strip()}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse DSN XML files in batches and write the extraced data to a prometheus data folder"
    )
    parser.add_argument("-l","--log",help="Loglevel",default="info")
    parser.add_argument("-c","--convert_only", action="store_true",help="Do not import into Prometheus")
    parser.add_argument("--input",help="Folder containing DSN XML zip files", default=in_dir)
    parser.add_argument("--output",help="Save OpenMetrics files to this directory, requires -c",default=out_dir)
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

    if args.output != out_dir and not args.convert_only:
        logger.error(f"Prometheus import only works for {out_dir}. Try adding -c flag.")
        exit(1)

    # Process input files
    start_processing_time = time.time()
    threads = []
    for f in glob.glob(path.join(args.input, '*')):
        date = path.basename(f)
        thread = threading.Thread(target=process_file, args=(f, date, out_dir))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    delta_processing_time = time.time() - start_processing_time
    logger.info(f"Converting to OpenMetrics took: {delta_processing_time} s")


    # Import OpenMetric files into Prometheus
    if not args.convert_only:
        start_import_time = time.time()
        import_all(out_dir, "1d")
        delta_import_time = time.time() - start_import_time
        logger.info(f"Importing OpenMetrics into Prometheus took: {delta_import_time} s")

    logger.info(f"Finished processing {args.input}")
