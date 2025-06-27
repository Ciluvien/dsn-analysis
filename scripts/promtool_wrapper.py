#!/usr/bin/env python3

import glob
import logging
from os import path
import subprocess
import argparse

INPUT_DIR = path.join("../data/openmetric/")
BLOCK_DURATION = "1d"

logger = logging.getLogger(__name__)

def import_all(directory, block_duration):
    # Create blocks from the temporary files
    for f in glob.glob(path.join(directory, '*')):
        logger.info(f"Creating blocks for {f}")

        # This assumes that the openmetrics directory is mounted at /openmetrics in the prometheus container
        docker_path = path.join("/openmetric/",path.basename(f))

        result = subprocess.run(
            ['docker','exec',"-it",'prometheus',
             'promtool', 'tsdb',
             'create-blocks-from', 'openmetrics',
             '--max-block-duration', block_duration,
             '-r', docker_path, "/prometheus/"]
            , capture_output=True, text=True)
        if result.stdout:
            logger.info(f"{result.stdout.strip()}")
        if result.stderr:
            logger.exception(f"{result.stderr.strip()}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Import OpenMetric data into a promtheus docker container"
    )
    parser.add_argument("-d","--directory",help="Directory to be imported", default=INPUT_DIR)
    parser.add_argument("-b","--block_duration",help="Maximum block duration", default=BLOCK_DURATION)
    parser.add_argument("-l","--log",help="Loglevel",default="info")
    args = parser.parse_args()

    if args.log:
        numeric_level = getattr(logging, args.log.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: %s' % args.log)
    else:
        numeric_level = logging.INFO
    logging.basicConfig(filename='.log', level=numeric_level)

    logger.info(f"Start parsing OpenMetric files at: {args.directory}")

    import_all(args.directory, args.block_duration)

    logger.info(f"Finished importing files in {args.directory}")
