import dropbox
from dropbox.exceptions import AuthError, ApiError, BadInputError
import os
from dotenv import load_dotenv
from dropbox.files import WriteMode
import datetime
import sys
import json

# --- Configuration and Setup ---
load_dotenv() # Load environment variables from .env file

# --- Credentials and IDs (Loaded from Environment Variables) ---
# These are needed within the upload function for initialization
DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
TEAM_SPACE_NAMESPACE_ID = os.getenv("TEAM_SPACE_NAMESPACE_ID")
TARGET_USER_MEMBER_ID = os.getenv("TARGET_USER_MEMBER_ID")

def upload_file_to_dropbox(local_file_path: str, dropbox_destination_path: str, add_timestamp: bool = True) -> str:
    """
    Initializes a Dropbox client with Team Space headers and uploads a local file,
    optionally adding a timestamp.

    Args:
        local_file_path: The path to the local file to upload.
        dropbox_destination_path: The full destination path in Dropbox (e.g., "/folder/filename.txt").
        add_timestamp: If True (default), appends '_YYYYMMDD_HHMMSS' before the extension.

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
        sys.exit(1) # Exit if essential creds are missing
    if not TEAM_SPACE_NAMESPACE_ID:
        print("Error: TEAM_SPACE_NAMESPACE_ID not set in environment variables.")
        sys.exit(1)
    if not TARGET_USER_MEMBER_ID or not TARGET_USER_MEMBER_ID.startswith("dbmid:"):
        print(f"Error: Invalid TARGET_USER_MEMBER_ID format: '{TARGET_USER_MEMBER_ID}'. Must start with 'dbmid:'.")
        sys.exit(1)

    # --- Validate Local File ---
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"Error: Local file not found at '{local_file_path}'")

    dbx = None # Initialize dbx to None
    try:
        # --- Initialize Client inside the function ---
        print(f"Initializing Dropbox client for Namespace ID {TEAM_SPACE_NAMESPACE_ID} and User {TARGET_USER_MEMBER_ID}")
        path_root_header_value = {".tag": "namespace_id", "namespace_id": TEAM_SPACE_NAMESPACE_ID}
        path_root_json_string = json.dumps(path_root_header_value)
        select_user_header_value = TARGET_USER_MEMBER_ID
        custom_headers = {
            "Dropbox-API-Path-Root": path_root_json_string,
            "Dropbox-API-Select-User": select_user_header_value
        }
        # print(f"Custom Headers being sent: {custom_headers}") # Optional: uncomment for debugging

        dbx = dropbox.Dropbox(
            app_key=DROPBOX_APP_KEY,
            app_secret=DROPBOX_APP_SECRET,
            oauth2_refresh_token=DROPBOX_REFRESH_TOKEN,
            headers=custom_headers
        )
        # Verify client connection
        # print("Verifying client connection...") # Optional: uncomment for debugging
        dbx.users_get_current_account()
        # print("Client initialized and verified successfully.") # Optional: uncomment for debugging

        # --- Prepare File and Path ---
        print(f"Reading local file: '{local_file_path}'")
        with open(local_file_path, 'rb') as f:
            file_content = f.read()

        folder_path = os.path.dirname(dropbox_destination_path)
        base_filename, ext = os.path.splitext(os.path.basename(dropbox_destination_path))

        # Ensure folder path starts with '/' and handle root '/' case
        if not folder_path.startswith('/'):
             folder_path = '/' + folder_path
        if folder_path != '/' and folder_path.endswith('/'):
            folder_path = folder_path.rstrip('/')

        # Construct final path
        if add_timestamp:
            # JST timezone
            jst = datetime.timezone(datetime.timedelta(hours=9))
            now_jst = datetime.datetime.now(jst)

            # Calculate yesterday's date
            yesterday_date = now_jst - datetime.timedelta(days=1)
            # Format as YYYYMMDD for yesterday's date ONLY
            timestamp = yesterday_date.strftime("%Y%m%d")

            # Construct the path using only yesterday's date timestamp
            final_dropbox_path = f"{folder_path}/{base_filename}_{timestamp}{ext}"
        else:
             # Ensure no double slashes if folder is root '/'
             if folder_path == '/':
                 final_dropbox_path = f"/{base_filename}{ext}"
             else:
                 final_dropbox_path = f"{folder_path}/{base_filename}{ext}"

        # --- Perform Upload ---
        print(f"Uploading {len(file_content)} bytes to final path: {final_dropbox_path}")
        # Use WriteMode('overwrite') to replace, or 'add' to fail if exists.
        metadata = dbx.files_upload(file_content, final_dropbox_path, mode=WriteMode('overwrite'))
        print(f"Successfully uploaded '{local_file_path}' to '{final_dropbox_path}'")
        print(f"File size on Dropbox: {metadata.size} bytes")
        return final_dropbox_path # Return the actual path used

    except (AuthError, ApiError, BadInputError) as e:
        print(f"Dropbox API or Authentication Error: {e}")
        # Handle specific errors if needed, e.g., conflict
        if isinstance(e, ApiError) and isinstance(e.error, dropbox.files.UploadWriteFailedError) and \
           e.error.reason.is_conflict():
            print("Upload Conflict: Check WriteMode settings ('add' vs 'overwrite') and existing files.")
        raise # Re-raise the exception
    except FileNotFoundError as e: # Already checked, but good practice
        print(f"File Error: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during upload: {e}")
        raise


# --- Example Usage (when script is run directly) ---
if __name__ == "__main__":
    print("--- Running Uploader Script as Main (Example) ---")
    # --- Example Parameters ---
    EXAMPLE_LOCAL_FILE = "test_uploader.txt"
    EXAMPLE_DROPBOX_DESTINATION = "/Uploaded Reports/example_main_report.txt" # Example full path

    # Create a dummy local file if it doesn't exist
    if not os.path.exists(EXAMPLE_LOCAL_FILE):
        print(f"Creating dummy local file: {EXAMPLE_LOCAL_FILE}")
        with open(EXAMPLE_LOCAL_FILE, "w") as f:
            f.write(f"Test content generated at {datetime.datetime.now()} for main execution.")

    # --- Perform Upload ---
    try:
        # Call the function directly, client initialization happens inside
        final_path = upload_file_to_dropbox(EXAMPLE_LOCAL_FILE, EXAMPLE_DROPBOX_DESTINATION, add_timestamp=True)
        print(f"\nExample upload completed. Final file path: {final_path}")
    except (FileNotFoundError, ApiError, AuthError, Exception) as e:
         print(f"\nEXAMPLE UPLOAD FAILED: {e}")
         sys.exit(1)
    finally:
        # Clean up dummy file
        if os.path.exists(EXAMPLE_LOCAL_FILE):
            #os.remove(EXAMPLE_LOCAL_FILE) # Uncomment to remove the test file after running
            pass

    print("\n--- Uploader script (main execution example) finished ---")