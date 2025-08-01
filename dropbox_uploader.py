import dropbox
from dropbox.exceptions import AuthError, ApiError, BadInputError
import os
from dotenv import load_dotenv
from dropbox.files import WriteMode
import datetime # Keep datetime if needed elsewhere, otherwise can remove
import sys
import json # Keep json if needed elsewhere, otherwise can remove

# --- Configuration and Setup ---
load_dotenv() # Load environment variables from .env file

# --- Credentials and IDs (Loaded from Environment Variables) ---
DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
# TEAM_SPACE_NAMESPACE_ID = os.getenv("TEAM_SPACE_NAMESPACE_ID") # No longer needed for personal space
TARGET_USER_MEMBER_ID = os.getenv("TARGET_USER_MEMBER_ID")

def upload_file_to_dropbox(local_file_path: str, dropbox_destination_path: str) -> str: 
    """
    Initializes a Dropbox client acting as a specific user and uploads a local file
    to that user's personal space at the specified destination path.

    Args:
        local_file_path: The path to the local file to upload.
        dropbox_destination_path: The full destination path in the target user's personal
                                  Dropbox space (e.g., "/folder/filename.txt").

    Returns:
        The final Dropbox path where the file was uploaded in the user's personal space.

    Raises:
        SystemExit: If required environment variables are missing or invalid.
        FileNotFoundError: If the local file does not exist.
        ApiError: If a Dropbox API error occurs during initialization or upload.
        AuthError: If there's an authentication issue with Dropbox credentials.
        Exception: For other unexpected errors.
    """
    print("\n--- Preparing Dropbox Upload to Personal Space ---")

    # --- Validate Credentials and IDs ---
    if not all([DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN]):
        print("Error: Dropbox credentials (APP_KEY, APP_SECRET, REFRESH_TOKEN) not found in environment variables.")
        sys.exit(1)
    # TEAM_SPACE_NAMESPACE_ID validation removed
    if not TARGET_USER_MEMBER_ID or not TARGET_USER_MEMBER_ID.startswith("dbmid:"):
        print(f"Error: Invalid TARGET_USER_MEMBER_ID format: '{TARGET_USER_MEMBER_ID}'. Must start with 'dbmid:'.")
        sys.exit(1)

    # --- Validate Local File ---
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"Error: Local file not found at '{local_file_path}'")

    dbx = None
    try:
        # --- Initialize Client ---
        # MODIFICATION: Remove the Dropbox-API-Path-Root header
        print(f"Initializing Dropbox client to act as User {TARGET_USER_MEMBER_ID} in their Personal Space")
        # path_root_header_value = {".tag": "namespace_id", "namespace_id": TEAM_SPACE_NAMESPACE_ID} # Removed
        # path_root_json_string = json.dumps(path_root_header_value) # Removed
        select_user_header_value = TARGET_USER_MEMBER_ID
        custom_headers = {
            # "Dropbox-API-Path-Root": path_root_json_string, # REMOVED THIS LINE
            "Dropbox-API-Select-User": select_user_header_value
        }

        dbx = dropbox.Dropbox(
            app_key=DROPBOX_APP_KEY,
            app_secret=DROPBOX_APP_SECRET,
            oauth2_refresh_token=DROPBOX_REFRESH_TOKEN,
            headers=custom_headers # Now only contains Select-User
        )
        dbx.users_get_current_account() # Verify client connection (will act as TARGET_USER_MEMBER_ID)

        # --- Prepare File and Path ---
        print(f"Reading local file: '{local_file_path}'")
        with open(local_file_path, 'rb') as f:
            file_content = f.read()

        # Ensure destination path starts with '/' (relative to user's personal root)
        final_dropbox_path = dropbox_destination_path
        if not final_dropbox_path.startswith('/Shimoyama_apps_personal'):
             final_dropbox_path = '/Shimoyama_apps_personal' + final_dropbox_path

        # Remove trailing slash if not root
        if final_dropbox_path != '/Shimoyama_apps_personal' and final_dropbox_path.endswith('/Shimoyama_apps_personal'):
            final_dropbox_path = final_dropbox_path.rstrip('/Shimoyama_apps_personal')


        # --- Perform Upload ---
        print(f"Uploading {len(file_content)} bytes to personal path: {final_dropbox_path} (as user {TARGET_USER_MEMBER_ID})")
        metadata = dbx.files_upload(file_content, final_dropbox_path, mode=WriteMode('overwrite'))
        print(f"Successfully uploaded '{local_file_path}' to user's personal space path '{final_dropbox_path}'")
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
    print("--- Running Uploader Script as Main (Example for Personal Space) ---")
    EXAMPLE_LOCAL_FILE = "test_uploader_personal.txt"
    # Destination path is now relative to the TARGET_USER_MEMBER_ID's personal Dropbox root
    EXAMPLE_DROPBOX_DESTINATION = "/My App Uploads/example_personal_report.txt"

    if not os.path.exists(EXAMPLE_LOCAL_FILE):
        print(f"Creating dummy local file: {EXAMPLE_LOCAL_FILE}")
        with open(EXAMPLE_LOCAL_FILE, "w") as f:
            f.write(f"Test content generated at {datetime.datetime.now()} for personal upload example.")

    try:
        # Call the modified function
        final_path = upload_file_to_dropbox_personal(EXAMPLE_LOCAL_FILE, EXAMPLE_DROPBOX_DESTINATION)
        print(f"\nExample personal upload completed. Final file path: {final_path}")
    except (FileNotFoundError, ApiError, AuthError, Exception) as e:
         print(f"\nEXAMPLE PERSONAL UPLOAD FAILED: {e}")
         sys.exit(1)
    finally:
        if os.path.exists(EXAMPLE_LOCAL_FILE):
            # os.remove(EXAMPLE_LOCAL_FILE) # Uncomment to clean up
            pass

    print("\n--- Uploader script (main execution example for personal) finished ---")