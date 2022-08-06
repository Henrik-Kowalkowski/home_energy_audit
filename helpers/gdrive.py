from typing import Union

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


def make_drive() -> object:
    """Instantiate google drive handler object.

    Returns:
        object: The authenticated drive handler object.
    """
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(credentials_file="my_creds.json")
    drive = GoogleDrive(gauth)
    return drive


def get_gdrive_ids(
    drive: object, locations: list, return_last: bool = True
) -> Union[dict, str]:
    """Get the directory ids up to and including the last file specified in the list of locations from google drive.

    Args:
        drive (object): The authenticated drive handler object.
        locations (list): The list of directories to search through.
        return_last (bool): Whether to return the full dict of location names and location ids or just the last as str.

    Returns:
        dict/str: The google drive ids of the directories specified. Or the final id of the path.
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

    if return_last:
        return file_dict[title]  # the file id of the file you are interested in
    else:
        return file_dict  # all of the dir names and ids leading up to the file you are interested in
