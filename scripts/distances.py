#!/usr/bin/env python3

import spiceypy as spice
import polars as pl
from collections import defaultdict
import datetime as dt
import multiprocessing as mp
from os import listdir, path
import argparse

KERNEL_DIR = path.join("../data/kernels/")
OUT_PATH = path.join("../data/")
THREAD_COUNT = mp.cpu_count()

# All DSN dish numbers
DSN_DISH_NUMS = [
    63,65,53,54,55,56,
    14,24,25,26,
    43,34,35,36
]

# 70-m DSN dish numbers to represent each of the three stations
DSN_STATIONS = [
    63,
    14,
    43
]

# Missions as extracted from a prometheus query: count(target_range_km) by (target_name)
# ONLY FOR REFERENCE, missing kernels
# MISSIONS = [
#     "ACE", "AGM1", "BEPI", "BIOS", "CAPS",
#     "CGO", "CHDR", "DSCO", "DSN", "DSS",
#     "EMM", "ESCB", "EURC", "GBRA", "GSSR",
#     "HYB2", "JNO", "JWST", "KPLO", "LRO",
#     "LTB", "LUCY", "M01O", "M20", "MEX",
#     "MMS1", "MMS2", "MMS3", "MMS4", "MRO",
#     "MSL", "MVN", "NHPC", "ORX", "PSYC",
#     "SOHO", "SPP", "STA", "SWFO", "TDR8",
#     "TESS", "TEST", "TGO", "THB", "THC",
#     "VGR1", "VGR2", "WIND", "XMM"
# ]
# # Remove bogus missions
# BOGUS = [
#     "TEST", "DSN", "DSS", "GBRA", "GSSR",
# ]
# MISSIONS_ALL = list(set(MISSIONS) - set(BOGUS))

# Missions for which kernels are available
MISSIONS = [
    "-49", "-61", "-170", "-159", "-121",
    "-53", "-202", "-168", "-41", "-255",
    "-64", "-96", "-98", "-76", "-74",
    "-31", "-32"
]

def process(ets, kernel_files):
    # Load necessary SPICE kernels
    for f in kernel_files:
      spice.furnsh(f)

    data = defaultdict(list)
    for t, et in ets.items():
            # Get the position of a DSN station
            station_pos_dict = {}
            for station in DSN_STATIONS:
                try:
                    station_pos_dict[station] = spice.spkpos(f"DSS-{station}", et, "J2000", "NONE", "EARTH")[0]
                except Exception:
                    print(f"Failed to compute position for {t}, {station}")

            # Get the position of a satellite
            sat_pos_dict = {}
            for sat in MISSIONS:
                try:
                    sat_pos_dict[sat] = spice.spkpos(sat, et, "J2000", "NONE", "EARTH")[0]
                except Exception:
                    print(f"Failed to compute position for {t}, id: {sat} \nRemoving {sat} from further processing")
                    # Assume that sat will not be available at future time stamps
                    MISSIONS.remove(sat)

            data["time"].extend([t]*len(station_pos_dict) * len(sat_pos_dict))
            for station, station_pos in station_pos_dict.items():
                for sat, sat_pos in sat_pos_dict.items():
                    data["station"].append(station)
                    data["target"].append(sat)
                    data["distance"].append(spice.vdist(station_pos, sat_pos))
    spice.kclear()
    return data


if __name__ == "__main__":
    start_time = dt.datetime(2025,5,1).timestamp()
    end_time = dt.datetime(2025,7,1).timestamp()

    parser = argparse.ArgumentParser(
        description="Calculate distances between DSN stations and targets"
    )
    parser.add_argument("--start",help="Start date", default=start_time)
    parser.add_argument("--end",help="End date", default=end_time)
    args = parser.parse_args()

    # Load leapsecond SPICE kernel
    f_path = path.join(KERNEL_DIR, "naif0012.tls")
    if path.isfile(f_path):
        spice.furnsh(f_path)

    # Define the time of interest
    ets = {t: spice.str2et(str(dt.datetime.fromtimestamp(t))) for t in range(int(args.start), int(args.end), 5) }

    # Load kernel files
    kernel_files = [path.join(KERNEL_DIR, f) for f in listdir(KERNEL_DIR) if path.isfile(path.join(KERNEL_DIR, f))]

    # Create a pool of processes
    with mp.Pool(processes=THREAD_COUNT) as pool:
        # Split the ets dictionary into chunks for each process
        part_size = int(len(ets) / THREAD_COUNT)
        ets_parts = [dict(list(ets.items())[i * part_size:(i + 1) * part_size]) for i in range(THREAD_COUNT)]
        # Ensure the last part gets any remaining items
        ets_parts[-1].update(dict(list(ets.items())[THREAD_COUNT * part_size:]))

        # Process the data in parallel
        results = pool.starmap(process, [(ets_part, kernel_files) for ets_part in ets_parts])

    data = defaultdict(list)
    for d in results:
        for key in d:
            data[key].extend(d[key])

    df = pl.DataFrame(data)

    for station in DSN_STATIONS:
        df.filter(pl.col("station") == station).write_csv(path.join(OUT_PATH,f"distances{station}.csv"), separator=",")
