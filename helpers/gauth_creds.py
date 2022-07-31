# https://stackoverflow.com/questions/24419188/automating-pydrive-verification-process

from pydrive.auth import GoogleAuth
import os, pathlib

home_dir = pathlib.Path(os.path.realpath("__file__")).parents[1]

gauth = GoogleAuth()
# Try to load saved client credentials
gauth.LoadCredentialsFile(home_dir / "my_creds.json")
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
gauth.SaveCredentialsFile(home_dir / "my_creds.json")
