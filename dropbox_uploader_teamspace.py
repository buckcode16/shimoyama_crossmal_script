import dropbox
from dropbox.exceptions import AuthError, ApiError, BadInputError
import os
from dotenv import load_dotenv
from dropbox.files import WriteMode
import datetime # Keep datetime if needed elsewhere, otherwise can remove
import sys
import json

# --- Configuration and Setup ---
load_dotenv() # Load environment variables from .env file

# --- Credentials and IDs (Loaded from Environment Variables) ---
DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
TEAM_SPACE_NAMESPACE_ID = os.getenv("TEAM_SPACE_NAMESPACE_ID")
TARGET_USER_MEMBER_ID = os.getenv("TARGET_USER_MEMBER_ID")

def upload_file_to_dropbox(local_file_path: str, dropbox_destination_path: str) -> str:
    """
    Initializes a Dropbox client with Team Space headers and uploads a local file
    to the specified destination path.

    Args:
        local_file_path: The path to the local file to upload.
        dropbox_destination_path: The full destination path in Dropbox (e.g., "/folder/filename.txt").

    Returns:
        The final Dropbox path where the file was uploaded.

    Raises:
        SystemExit: If required environment variables are missing or invalid.
        FileNotFoundError: If the local file does not exist.
        ApiError: If a Dropbox API error occurs during initialization or upload.
        AuthError: If there's an authentication issue with Dropbox credentials.
        Exception: For other unexpected errors.
    """
    print("\n--- Preparing Dropbox Upload ---")

    # --- Validate Credentials and IDs ---
    if not all([DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN]):
        print("Error: Dropbox credentials (APP_KEY, APP_SECRET, REFRESH_TOKEN) not found in environment variables.")
        sys.exit(1)
    if not TEAM_SPACE_NAMESPACE_ID:
        print("Error: TEAM_SPACE_NAMESPACE_ID not set in environment variables.")
        sys.exit(1)
    if not TARGET_USER_MEMBER_ID or not TARGET_USER_MEMBER_ID.startswith("dbmid:"):
        print(f"Error: Invalid TARGET_USER_MEMBER_ID format: '{TARGET_USER_MEMBER_ID}'. Must start with 'dbmid:'.")
        sys.exit(1)

    # --- Validate Local File ---
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"Error: Local file not found at '{local_file_path}'")

    dbx = None
    try:
        # --- Initialize Client ---
        print(f"Initializing Dropbox client for Namespace ID {TEAM_SPACE_NAMESPACE_ID} and User {TARGET_USER_MEMBER_ID}")
        path_root_header_value = {".tag": "namespace_id", "namespace_id": TEAM_SPACE_NAMESPACE_ID}
        path_root_json_string = json.dumps(path_root_header_value)
        select_user_header_value = TARGET_USER_MEMBER_ID
        custom_headers = {
            "Dropbox-API-Path-Root": path_root_json_string,
            "Dropbox-API-Select-User": select_user_header_value
        }

        dbx = dropbox.Dropbox(
            app_key=DROPBOX_APP_KEY,
            app_secret=DROPBOX_APP_SECRET,
            oauth2_refresh_token=DROPBOX_REFRESH_TOKEN,
            headers=custom_headers
        )
        dbx.users_get_current_account() # Verify client connection

        # --- Prepare File and Path ---
        print(f"Reading local file: '{local_file_path}'")
        with open(local_file_path, 'rb') as f:
            file_content = f.read()

        # Ensure destination path starts with '/'
        final_dropbox_path = dropbox_destination_path
        if not final_dropbox_path.startswith('/'):
             final_dropbox_path = '/' + final_dropbox_path

        # Remove trailing slash if not root
        if final_dropbox_path != '/' and final_dropbox_path.endswith('/'):
            final_dropbox_path = final_dropbox_path.rstrip('/')


        # --- Perform Upload ---
        print(f"Uploading {len(file_content)} bytes to final path: {final_dropbox_path}")
        metadata = dbx.files_upload(file_content, final_dropbox_path, mode=WriteMode('overwrite'))
        print(f"Successfully uploaded '{local_file_path}' to '{final_dropbox_path}'")
        print(f"File size on Dropbox: {metadata.size} bytes")
        return final_dropbox_path # Return the actual path used

    except (AuthError, ApiError, BadInputError) as e:
        print(f"Dropbox API or Authentication Error: {e}")
        if isinstance(e, ApiError) and isinstance(e.error, dropbox.files.UploadWriteFailedError) and \
           e.error.reason.is_conflict():
            print("Upload Conflict: Check WriteMode settings ('add' vs 'overwrite') and existing files.")
        raise
    except FileNotFoundError as e:
        print(f"File Error: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during upload: {e}")
        raise


# --- Example Usage (when script is run directly) ---
if __name__ == "__main__":
    print("--- Running Uploader Script as Main (Example) ---")
    EXAMPLE_LOCAL_FILE = "test_uploader.txt"
    # Now the destination path is exactly what you want it to be in Dropbox
    EXAMPLE_DROPBOX_DESTINATION = "/Uploaded Reports/example_main_report_no_timestamp.txt"

    if not os.path.exists(EXAMPLE_LOCAL_FILE):
        print(f"Creating dummy local file: {EXAMPLE_LOCAL_FILE}")
        with open(EXAMPLE_LOCAL_FILE, "w") as f:
            f.write(f"Test content generated at {datetime.datetime.now()} for main execution.")

    try:
        # Call the function - no add_timestamp argument anymore
        final_path = upload_file_to_dropbox(EXAMPLE_LOCAL_FILE, EXAMPLE_DROPBOX_DESTINATION)
        print(f"\nExample upload completed. Final file path: {final_path}")
    except (FileNotFoundError, ApiError, AuthError, Exception) as e:
         print(f"\nEXAMPLE UPLOAD FAILED: {e}")
         sys.exit(1)
    finally:
        if os.path.exists(EXAMPLE_LOCAL_FILE):
            # os.remove(EXAMPLE_LOCAL_FILE) # Uncomment to clean up
            pass

    print("\n--- Uploader script (main execution example) finished ---")