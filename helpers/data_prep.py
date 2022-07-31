# GOAL: clean and combine NOAA data for analysis so it does not need to be re-run with analytics

import os, pathlib
import re

# Define data read and write locations
raw_dir = pathlib.Path(os.path.realpath("__file__")).parents[1] / "data" / "raw"
clean_dir = pathlib.Path(os.path.realpath("__file__")).parents[1] / "data" / "clean"

# Open and clean NOAA fields
with open(raw_dir / "noaa" / "headers.txt", mode="r") as f:
    noaa_headers = [l.strip() for l in f.readlines()]
    noaa_fields = [l.lower() for l in noaa_headers[1].split(" ")]

# Open and clean NOAA data
## Data is has extra spaces
## Replace extra spaces with commas and save as .csv
with open(raw_dir / "noaa" / "CRNH0203-2022-MN_Sandstone_6_W.txt", mode="r") as f:
    noaa_data = re.sub(pattern=" +", repl=",", string=f.read())

# Write cleaned data to file
with open(clean_dir / "noaa_data.csv", mode="w") as f:
    # Add field names to data
    noaa_data = ",".join(noaa_fields) + "\n" + noaa_data
    f.write(noaa_data)
