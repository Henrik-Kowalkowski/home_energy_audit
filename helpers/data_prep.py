# GOAL: clean and combine NOAA data for analysis so it does not need to be re-run with analytics

# Data manipulation
import pandas as pd

# OS
import os, pathlib
import re
from io import StringIO

# Google Drive
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# Define data read and write locations
home_dir = pathlib.Path(os.path.realpath("__file__")).parents[1]
raw_dir = home_dir / "data" / "raw"
clean_dir = home_dir / "data" / "clean"

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

# Gdrive connnections
def get_file(locations: list) -> list:
    """Get the directory ids up to and including the last file specified in the list of locations from google drive.

    Args:
        locations (list): The list of directories to search through.

    Returns:
        list: The google drive ids of the directories specified.
    """
    locations = locations.copy()
    file_dict = {"root": "root"}
    title = "root"
    while len(locations) > 0:
        file_list = drive.ListFile(
            {"q": f"'{file_dict[title]}' in parents and trashed=false"}
        ).GetList()
        title = locations.pop(0)
        for file in file_list:
            if file["title"] == title:
                file_dict[title] = file["id"]

    return file_dict


gauth = GoogleAuth()

gauth.LoadCredentialsFile("my_creds.json")

drive = GoogleDrive(gauth)

# Sense data
locations = ["Data", "home_energy_audit", "sense_energy_data_2022.csv"]
sense_data_id = get_file(locations)[locations[-1]]

file = drive.CreateFile({"id": sense_data_id})
sense_data_string = file.GetContentString()

sense_data = pd.read_csv(StringIO(sense_data_string), skiprows=0, header=1)
sense_data.head()

sense_data.to_csv(clean_dir / "private_sense_energy_data.csv")

# Nest data
nest_datasets = {}
locations = [
    "Data",
    "home_energy_audit",
    "nest_thermostat_data_2022",
    "Nest",
    "thermostats",
    "09AA01AC481614FL",
    "2022",
]
for i in range(1, 8):
    nest_folder_name = f"0{i}"
    nest_data_name = f"2022-{nest_folder_name}-sensors.csv"
    full_location = locations.copy() + [nest_folder_name, nest_data_name]
    nest_data_id = get_file(full_location)[full_location[-1]]
    nest_datasets[nest_data_name] = nest_data_id

# file = drive.CreateFile({"id": sense_data_id})
# sense_data_string = file.GetContentString()

# sense_data = pd.read_csv(
#     StringIO(sense_data_string),
#     skiprows=0,
#     header=1,
#     sep=",",
#     parse_dates=[0],
#     infer_datetime_format=True,
# )
# sense_data.head()
