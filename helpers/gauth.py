# https://stackoverflow.com/questions/24419188/automating-pydrive-verification-process

from pydrive.auth import GoogleAuth
import pathlib, re


def make_drive_creds(project_path: pathlib.Path, project_name: str) -> None:
    """Create or access Google Drive credentials.

    Args:
        project_path (object): Pathlib.Path project path.
        project_name (str): Name of project. e.g. home_energy_audit.
    """

    creds_filename = "my_creds.json"

    gauth = GoogleAuth()
    # Try to load saved client credentials
    gauth.LoadCredentialsFile(project_path / creds_filename)
    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
    else:
        # Initialize the saved creds
        gauth.Authorize()
    # Save the current credentials to a file
    gauth.SaveCredentialsFile(project_path / creds_filename)

    pattern = f"^.*{project_name}"
    safe_path = re.sub(pattern, project_name, str(project_path))

    print(f"Credentials saved to {safe_path} as {creds_filename}")
