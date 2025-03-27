import os
import datetime
import dropbox
from dropbox.files import WriteMode
from dotenv import load_dotenv

load_dotenv()  

DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")

def upload_file_to_dropbox(local_file, dropbox_destination):
    dbx = dropbox.Dropbox(
        app_key=DROPBOX_APP_KEY,
        app_secret=DROPBOX_APP_SECRET,
        oauth2_refresh_token=DROPBOX_REFRESH_TOKEN
    )

    with open(local_file, 'rb') as f:
        file_content = f.read()

    base, ext = os.path.splitext(dropbox_destination)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    new_destination = f"{base}_{timestamp}{ext}"

    try:
        dbx.files_upload(file_content, new_destination, mode=WriteMode('add'))
        print(f"Successfully uploaded {local_file} to {new_destination}")
    except Exception as e:
        print(f"Failed to upload {local_file}: {e}")
        raise

# Example usage
# if __name__ == "__main__":
#     LOCAL_FILE = "example.xml"
#     DROPBOX_DEST = "/Reports/output.xml"
#     upload_file_to_dropbox(LOCAL_FILE, DROPBOX_DEST)
