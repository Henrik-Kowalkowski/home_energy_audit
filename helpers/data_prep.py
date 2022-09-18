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

for k in nest_data[0]['2022-01-02T00:00:00Z'].keys():
    print(k, ":", type(nest_data[0]['2022-01-02T00:00:00Z'][k]))