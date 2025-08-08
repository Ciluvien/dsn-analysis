#!/usr/bin/env python3
import re
from collections import defaultdict
import xmltodict
import json
import argparse
from os import path, listdir, mkdir
from shutil import rmtree
import tempfile
import zipfile
from random import randint
import logging

logger = logging.getLogger(__name__)

def rewrite(xml_string):
    # Move all dishes inside of their respective station element
    stations_cleaned = []
    close_station = False
    for line in xml_string:
        if ("station" in line or "timestamp" in line) and close_station:
            stations_cleaned.append("</station>")
            close_station = False

        if "station" in line:
            stations_cleaned.append(line.replace("/>",">"))
            close_station = True
        else:
            stations_cleaned.append(line)

    # Move all signals inside of their respective target element
    targets_cleaned = []
    signals = defaultdict(list)
    targets = {}
    for line in stations_cleaned:
        if "</dish" in line:
            for t_id, t_line in targets.items():
                if not signals.get(t_id):
                    targets_cleaned.append(t_line)
                    continue
                else:
                    targets_cleaned.append(t_line.replace("/>",">"))
                for signal in signals.get(t_id, []):
                    targets_cleaned.append(signal)
                targets_cleaned.append("</target>")
            signals = defaultdict(list)
            targets = {}
            targets_cleaned.append(line)
        elif "Signal" in line:
            spacecraftID = re.search(r'spacecraftID="-(\d*)"', line)
            if spacecraftID:
                signals[spacecraftID.group(1)].append(line)
        elif "target" in line:
            t_id = re.search(r'id="(\d*)"', line)
            if t_id:
                targets[t_id.group(1)] = line
        else:
            targets_cleaned.append(line)

    return targets_cleaned

# Returns the given xml file converted to a python dictionary
def xml_path_to_dict(xml):
    with open(xml) as xml_file:
        xml_string = xml_file.readlines()

    # Pretty logger.info xml
    # xml_tree = ET.fromstringlist(canonify(xml_string))
    # ET.indent(xml_tree)
    # logger.info(ET.tostring(xml_tree, encoding='unicode'))

    try:
        parsed = xmltodict.parse("".join(rewrite(xml_string)))
    except Exception:
        logger.info(f"Failed to parse: {xml}")
        parsed = None
    return parsed

def process_batch(directory: str) -> list[str]:
    if directory[-1] != "/":
        directory += "/"
    files = [f"{directory}{f}" for f in listdir(directory) if path.isfile(f"{directory}{f}")]

    result = []
    for f in files:
        with open(f) as xml:
            dic = xml_path_to_dict(xml.read())
            if dic:
                result.append(dic)

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = "Rewrites DSN Now XML files to sensible json",
    )
    parser.add_argument("-p","--pretty",action='store_true', help="indent json for better viewing at the cost of file size")
    parser.add_argument("-b","--batch",action='store_true')
    parser.add_argument("-l","--log",help="Loglevel")
    parser.add_argument("input")
    parser.add_argument("output")
    args = parser.parse_args()

    if args.log:
        numeric_level = getattr(logging, args.log.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: %s' % args.log)
    else:
        numeric_level = logging.INFO
    logging.basicConfig(level=numeric_level)

    if args.batch:
        if path.isdir(args.input):
            result = process_batch(args.input)

        elif path.isfile(args.input) and ".zip" in args.input:
            temp_dir_base = tempfile.gettempdir() + "/dsn_rewrite_extracted"
            suffix=randint(0,999999)
            temp_dir = f"{temp_dir_base}{suffix:06d}"
            while path.isdir(temp_dir):
                suffix=randint(0,999999)
                temp_dir=f"{temp_dir_base}{suffix:06d}"
            mkdir(temp_dir)
            with zipfile.ZipFile(args.input, "r") as zipf:
                zipf.extractall(temp_dir)
            result = process_batch(temp_dir)
            if path.isdir(temp_dir):
                rmtree(temp_dir)
        else:
            logger.info("Could not process input path")
            exit(1)

        # TODO: find way to store output files in batch processing
        logger.info(result)
    else:
        dic = xml_path_to_dict(args.input)
        with open(args.output, "w") as json_file:
            if args.pretty:
                json_file.write(json.dumps(dic, indent=4))
            else:
                json_file.write(json.dumps(dic))
