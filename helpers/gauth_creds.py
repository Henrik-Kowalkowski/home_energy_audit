# https://stackoverflow.com/questions/24419188/automating-pydrive-verification-process

from pydrive.auth import GoogleAuth

# NOTE: client_secrets.json from google api must live in here
gauth = GoogleAuth()
# Try to load saved client credentials
gauth.LoadCredentialsFile("my_creds.json")
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
gauth.SaveCredentialsFile("my_creds.json")
