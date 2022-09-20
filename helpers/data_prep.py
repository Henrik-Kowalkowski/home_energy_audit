# Data manipulation
import pandas as pd
import json
import numpy as np

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
    noaa_headers = [f"noaa_{col}" for col in noaa_headers]

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
    sense_data.columns = [
        f"sense_{col}" for col in sense_data.columns.str.lower().str.replace(" ", "_")
    ]

    return sense_data


def get_nest_data(drive: object) -> tuple():
    """Download nest data from google drive.

    Args:
        drive (object): The authenticated drive handler object.

    Returns:
        tuple[dataframe, dataframe, dataframe]: The nest sensor dataframe, cycle dataframe, and event dataframe.
    """

    locations = [
        "Data",
        "home_energy_audit",
        "nest_thermostat_data_2022",
        "Nest",
        "thermostats",
        "09AA01AC481614FL",
        "2022",
    ]

    # Get ids of csv files
    nest_sensor_data_ids = {}

    ## Nest packages the files into monthly breakdowns
    for i in range(1, 8):
        nest_folder_name = f"0{i}"
        nest_sensor_data_name = f"2022-{nest_folder_name}-sensors.csv"
        full_location = locations.copy() + [nest_folder_name, nest_sensor_data_name]
        nest_sensor_data_id = get_gdrive_ids(drive, full_location)
        nest_sensor_data_ids[nest_sensor_data_name] = nest_sensor_data_id

    # Load monthly Nest sensor files
    nest_sensor_data = []
    for nest_sensor_data_id in nest_sensor_data_ids.values():
        file = drive.CreateFile({"id": nest_sensor_data_id})
        nest_sensor_data_string = file.GetContentString()
        nest_sensor_data.append(pd.read_csv(StringIO(nest_sensor_data_string)))

    # Combine monthly Nest files and clean column names
    nest_sensor_data = pd.concat(nest_sensor_data)
    nest_sensor_data.columns = [
        f"nest_{col}"
        for col in nest_sensor_data.columns.str.replace("(", "_", regex=True)
        .str.replace(")", "", regex=True)
        .str.lower()
    ]

    # Get ids of json files
    nest_summary_data_ids = {}

    ## Nest packages the files into monthly breakdowns
    for i in range(1, 8):
        nest_folder_name = f"0{i}"
        nest_summary_data_name = f"2022-{nest_folder_name}-summary.json"
        full_location = locations.copy() + [nest_folder_name, nest_summary_data_name]
        nest_summary_data_id = get_gdrive_ids(drive, full_location)
        nest_summary_data_ids[nest_summary_data_name] = nest_summary_data_id

    # Load monthly Nest files
    nest_summary_data = []
    for nest_summary_data_id in nest_summary_data_ids.values():
        file = drive.CreateFile({"id": nest_summary_data_id})
        nest_summary_data_string = file.GetContentString()
        nest_summary_data.append(json.loads(nest_summary_data_string))

    # Declare mapping structure
    nest_cycle_data = {
        "nest_start_ts": [],
        "nest_end_ts": [],
        "nest_heat1": [],
        "nest_cool1": [],
        "nest_fan": [],
        "nest_caption": [],
    }

    # Traverse summary data and extract datapoints
    for month in nest_summary_data:
        for day in month:
            for cycle in month[day]["cycles"]:
                nest_cycle_data["nest_start_ts"].append(
                    cycle["caption"]["parameters"]["startTime"]
                )
                nest_cycle_data["nest_end_ts"].append(
                    cycle["caption"]["parameters"]["endTime"]
                )
                nest_cycle_data["nest_heat1"].append(cycle["heat1"])
                nest_cycle_data["nest_cool1"].append(cycle["cool1"])
                nest_cycle_data["nest_fan"].append(cycle["fan"])
                nest_cycle_data["nest_caption"].append(cycle["caption"]["plainText"])

    nest_cycle_data = pd.DataFrame(nest_cycle_data)
    nest_cycle_data["nest_start_ts"] = pd.to_datetime(
        nest_cycle_data.nest_start_ts
    ).dt.tz_convert("US/Central")

    # Declare mapping structure
    nest_event_data = {
        "nest_event_ts": [],
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

    # Traverse summary data and extract datapoints if available
    for month in nest_summary_data:
        for day in month:
            for event in month[day]["events"]:
                nest_event_data["nest_event_ts"].append(event["startTs"])
                nest_event_data["nest_event_type"].append(event["eventType"])
                try:
                    nest_event_data["nest_set_point_type"].append(
                        event["setPoint"]["setPointType"]
                    )
                    nest_event_data["nest_set_point_schedule_type"].append(
                        event["setPoint"]["scheduleType"]
                    )
                    nest_event_data["nest_heating_target"].append(
                        event["setPoint"]["targets"]["heatingTarget"]
                    )
                    nest_event_data["nest_cooling_target"].append(
                        event["setPoint"]["targets"]["coolingTarget"]
                    )
                    nest_event_data["nest_touched_by"].append(
                        event["setPoint"]["touchedBy"]
                    )
                    nest_event_data["nest_touched_where"].append(
                        event["setPoint"]["touchedWhere"]
                    )
                # Some events do not have a setPoint
                except:
                    nest_event_data["nest_set_point_type"].append("na")
                    nest_event_data["nest_set_point_schedule_type"].append("na")
                    nest_event_data["nest_heating_target"].append("na")
                    nest_event_data["nest_cooling_target"].append("na")
                    nest_event_data["nest_touched_by"].append("na")
                    nest_event_data["nest_touched_where"].append("na")

    # Convert data to DataFrame
    nest_event_data = pd.DataFrame(nest_event_data)
    nest_event_data = nest_event_data.replace("na", np.nan)
    nest_event_data["nest_event_ts"] = pd.to_datetime(
        nest_event_data.nest_event_ts
    ).dt.tz_convert("US/Central")

    return nest_sensor_data, nest_cycle_data, nest_event_data
