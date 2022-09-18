# Data manipulation
import pandas as pd

# Strings
import re
from io import StringIO

# Google Drive
from .gdrive import get_gdrive_ids


def get_noaa_data(drive: object) -> tuple:
    """Download noaa data and readme from google drive.

    Args:
        drive (object): The authenticated drive handler object.

    Returns:
        tuple[dataframe, str]: The noaa dataframe and noaa readme.
    """

    # List out data location
    noaa_locations = [
        "Data",
        "home_energy_audit",
        "noaa_weather_data_2022",
    ]

    # Get id of csv file
    noaa_data_id = get_gdrive_ids(
        drive, noaa_locations + ["CRNH0203-2022-MN_Sandstone_6_W.txt"]
    )

    # Clean headers and readme
    noaa_headers_id = get_gdrive_ids(drive, noaa_locations + ["headers.txt"])
    noaa_readme_id = get_gdrive_ids(drive, noaa_locations + ["readme.txt"])

    # Load noaa data
    file = drive.CreateFile({"id": noaa_data_id})
    noaa_data_string = file.GetContentString()
    ## Convert noaa data to string csv
    noaa_data = re.sub(pattern=" +", repl=",", string=noaa_data_string)

    # Load noaa headers
    file = drive.CreateFile({"id": noaa_headers_id})
    noaa_headers_string = file.GetContentString()
    ## Clean noaa headers
    noaa_headers = pd.Series(
        noaa_headers_string.split("\n")[1].strip().split(" ")
    ).str.lower()

    # Load noaa readme
    file = drive.CreateFile({"id": noaa_readme_id})
    noaa_readme_string = file.GetContentString()

    # Package noaa data as dataframe
    ## Note the first 8 columns should be str type
    noaa_data = pd.read_csv(
        StringIO(noaa_data), names=noaa_headers, dtype={i: "str" for i in range(8)}
    )

    return noaa_data, noaa_readme_string


def get_sense_data(drive: object) -> pd.DataFrame:
    """Download sense data from google drive.

    Args:
        drive (object): The authenticated drive handler object.

    Returns:
        dataframe: The sense dataframe.
    """

    # Get id of csv file
    sense_locations = [
        "Data",
        "home_energy_audit",
        "sense_energy_data_2022",
        "sense_energy_data_2022.csv",
    ]
    sense_data_id = get_gdrive_ids(drive, sense_locations)

    # Load sense data
    file = drive.CreateFile({"id": sense_data_id})
    sense_data_string = file.GetContentString()

    # Package noaa data as dataframe
    sense_data = pd.read_csv(
        StringIO(sense_data_string),
        skiprows=0,
        header=1,
        parse_dates=[0],
        infer_datetime_format=True,
    )
    sense_data.columns = sense_data.columns.str.lower().str.replace(" ", "_")

    return sense_data


def get_nest_data(drive: object) -> pd.DataFrame:
    """Download nest data from google drive.

    Args:
        drive (object): The authenticated drive handler object.

    Returns:
        dataframe: The nest dataframe.
    """

    # Get ids of csv files
    nest_data_ids = {}
    locations = [
        "Data",
        "home_energy_audit",
        "nest_thermostat_data_2022",
        "Nest",
        "thermostats",
        "09AA01AC481614FL",
        "2022",
    ]
    ## Nest packages the files into monthly breakdowns
    for i in range(1, 8):
        nest_folder_name = f"0{i}"
        nest_data_name = f"2022-{nest_folder_name}-sensors.csv"
        full_location = locations.copy() + [nest_folder_name, nest_data_name]
        nest_data_id = get_gdrive_ids(drive, full_location)
        nest_data_ids[nest_data_name] = nest_data_id

    # Load monthly Nest sensor files
    nest_data = []
    for nest_data_id in nest_data_ids.values():
        file = drive.CreateFile({"id": nest_data_id})
        nest_data_string = file.GetContentString()
        nest_data.append(pd.read_csv(StringIO(nest_data_string)))

    # Combine monthly Nest files and clean column names
    nest_data = pd.concat(nest_data)
    nest_data.columns = (
        nest_data.columns.str.replace("(", "_", regex=True)
        .str.replace(")", "", regex=True)
        .str.lower()
    )

    nest_sensor_data = nest_data.copy()

    return nest_sensor_data, nest_summary_data


from gdrive import make_drive
from gdrive import get_gdrive_ids
import pathlib, os
import json
import pandas as pd
import numpy as np

home_dir = pathlib.Path(os.path.realpath("__file__")).parents[1]
drive = make_drive(home_dir)


nest_data_ids = {}
locations = [
    "Data",
    "home_energy_audit",
    "nest_thermostat_data_2022",
    "Nest",
    "thermostats",
    "09AA01AC481614FL",
    "2022",
]
## Nest packages the files into monthly breakdowns
for i in range(1, 8):
    nest_folder_name = f"0{i}"
    nest_data_name = f"2022-{nest_folder_name}-summary.json"
    full_location = locations.copy() + [nest_folder_name, nest_data_name]
    nest_data_id = get_gdrive_ids(drive, full_location)
    nest_data_ids[nest_data_name] = nest_data_id

# Load monthly Nest files
nest_data = []
for nest_data_id in nest_data_ids.values():
    file = drive.CreateFile({"id": nest_data_id})
    nest_data_string = file.GetContentString()
    nest_data.append(json.loads(nest_data_string))

event_data = {
    "nest_start_ts": [],
    "nest_duration": [],
    "nest_event_type": [],
    "nest_set_point_type": [],
    "nest_set_point_schedule_type": [],
    "nest_heating_target": [],
    "nest_cooling_target": [],
    "nest_touched_ts": [],
    "nest_touched_by": [],
    "nest_touched_where": [],
}
for month in nest_data:
    for day in month:
        for event in month[day]["events"]:
            event_data["nest_start_ts"].append(event["startTs"])
            event_data["nest_duration"].append(event["duration"])
            event_data["nest_event_type"].append(event["eventType"])
            try:
                event_data["nest_set_point_type"].append(
                    event["setPoint"]["setPointType"]
                )
                event_data["nest_set_point_schedule_type"].append(
                    event["setPoint"]["scheduleType"]
                )
                event_data["nest_heating_target"].append(
                    event["setPoint"]["targets"]["heatingTarget"]
                )
                event_data["nest_cooling_target"].append(
                    event["setPoint"]["targets"]["coolingTarget"]
                )
                event_data["nest_touched_ts"].append(event["setPoint"]["touchedWhen"])
                event_data["nest_touched_by"].append(event["setPoint"]["touchedBy"])
                event_data["nest_touched_where"].append(
                    event["setPoint"]["touchedWhere"]
                )
            except:
                event_data["nest_set_point_type"].append("na")
                event_data["nest_set_point_schedule_type"].append("na")
                event_data["nest_heating_target"].append("na")
                event_data["nest_cooling_target"].append("na")
                event_data["nest_touched_ts"].append("na")
                event_data["nest_touched_by"].append("na")
                event_data["nest_touched_where"].append("na")

nest_summary_data = pd.DataFrame(event_data)
nest_summary_data = nest_summary_data.replace("na", np.nan)
nest_summary_data["nest_start_ts"] = pd.to_datetime(
    nest_summary_data.nest_start_ts
).dt.tz_convert("US/Central")
nest_summary_data["nest_touched_ts"] = pd.to_datetime(
    nest_summary_data.nest_touched_ts
).dt.tz_convert("US/Central")
nest_summary_data.dtypes
nest_summary_data[
    ["nest_event_type", "nest_set_point_type", "nest_set_point_schedule_type"]
].value_counts()

event = nest_data[0]["2022-01-02T00:00:00Z"]["events"][1]
event["setPoint"]["setPointType"]

for k in nest_data[0]["2022-01-02T00:00:00Z"].keys():
    print(k, ":", type(nest_data[0]["2022-01-02T00:00:00Z"][k]))

import pytz

pytz.all_timezones
