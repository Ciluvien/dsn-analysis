#!/usr/bin/env python3

from os import path, mkdir
import subprocess
import glob
import threading
import logging
import argparse
from promtool_wrapper import import_all

logger = logging.getLogger(__name__)

# home_dir = path.expanduser("~")

# Path to the temporary OpenMetrics files
temp_dir = path.join("../data/openmetric/")
# Path to the required DSN XML files
in_dir = path.join("../data/to_be_imported/")
# Path to the prometheus data folder (inside the prometheus container)
out_dir = path.join("/prometheus/")

# Semaphore to limit the number of concurrent subprocesses
max_concurrent_processes = 3
semaphore = threading.Semaphore(max_concurrent_processes)

def process_file(f, date):
    om_file = path.join(temp_dir, f'dsn_{date}.om')
    with semaphore:
        logger.info(f"Start processing {f}")
        result = subprocess.run(['python', 'openmetrify.py', '-l', 'INFO', '-b', '-x', f, om_file], capture_output=True, text=True)
        if result.stdout:
            logger.info(f"{result.stdout.strip()}")
        if result.stderr:
            logger.exception(f"{result.stderr.strip()}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse DSN XML files in batches and write the extraced data to a prometheus data folder"
    )
    parser.add_argument("-l","--log",help="Loglevel",default="info")
    parser.add_argument("-s","--skip", action="store_true",help="Skip processiong, only batch import")
    parser.add_argument("--om_dir",help="Save OpenMetrics files to this directory",default=None)
    parser.add_argument("--input",help="Folder containing DSN XML zip files", default=in_dir)
    parser.add_argument("--output",help="Prometheus data folder", default=out_dir)
    args = parser.parse_args()


    if not path.isdir(args.input):
        logger.error("Input must be a directory")
        exit(1)


    if args.om_dir:
        if not path.isdir(args.om_dir):
            logger.error("om_dir must be a directory")
            exit(1)
        else:
            temp_dir = args.om_dir

    if args.log:
        numeric_level = getattr(logging, args.log.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: %s' % args.log)
    else:
        numeric_level = logging.INFO
    logging.basicConfig(filename='.log', level=numeric_level)

    logger.info(f"Start parsing DSN XML zip files at: {args.input}")


    if not path.isdir(temp_dir):
        mkdir(temp_dir)

    if not args.skip:
        # Process input files
        threads = []
        for f in glob.glob(path.join(args.input, '*')):
            date = path.basename(f)
            thread = threading.Thread(target=process_file, args=(f, date))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    import_all(temp_dir, "1d")

    logger.info(f"Finished processing {args.input}")
