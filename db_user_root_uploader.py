import os
import datetime
import dropbox
from dropbox.files import WriteMode, FolderMetadata, FileMetadata
from dropbox.exceptions import AuthError, ApiError
from dotenv import load_dotenv
import uuid

load_dotenv()

DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")

# --- Dropbox Client Initialization ---
def get_dropbox_client():
    """Initializes and returns a Dropbox client instance."""
    if not all([DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN]):
        raise ValueError("Dropbox credentials (APP_KEY, APP_SECRET, REFRESH_TOKEN) are not set in environment variables.")
    try:
        dbx = dropbox.Dropbox(
            app_key=DROPBOX_APP_KEY,
            app_secret=DROPBOX_APP_SECRET,
            oauth2_refresh_token=DROPBOX_REFRESH_TOKEN
        )
        dbx.users_get_current_account()
        print("Dropbox authentication successful.")
        return dbx
    except AuthError as e:
        print(f"Dropbox Authentication Failed: {e}")
        raise
    except Exception as e:
        print(f"Failed to initialize Dropbox client or authenticate: {e}")
        raise

# --- Upload Function (same as before) ---
def upload_file_to_dropbox(dbx: dropbox.Dropbox, local_file: str, dropbox_destination: str) -> str:
    """Uploads file, adds timestamp, returns final path."""
    if not os.path.exists(local_file):
        raise FileNotFoundError(f"Local file not found: {local_file}")

    print(f"\nAttempting to upload '{local_file}' to Dropbox destination folder '{os.path.dirname(dropbox_destination)}'")

    try:
        with open(local_file, 'rb') as f:
            file_content = f.read()

        base, ext = os.path.splitext(dropbox_destination)

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        if not base.startswith('/'):
            base = '/' + base
        new_destination = f"{base}_{timestamp}{ext}"

        print(f"Uploading {len(file_content)} bytes to: {new_destination}")
        metadata = dbx.files_upload(file_content, new_destination, mode=WriteMode('add'))
        print(f"Successfully uploaded '{local_file}' to '{new_destination}'")
        print(f"File size on Dropbox: {metadata.size} bytes")
        return new_destination
    except ApiError as e:
        print(f"Dropbox API Error during upload: {e}")
        raise
    except Exception as e:
        print(f"Failed to upload '{local_file}': {e}")
        raise

def list_dropbox_path(dbx: dropbox.Dropbox, dropbox_path: str):
    """Lists contents of a Dropbox path."""
    path_to_list = dropbox_path if dropbox_path != "/" else ""
    if path_to_list and not path_to_list.startswith('/'):
        path_to_list = '/' + path_to_list

    print(f"\nListing contents of Dropbox path: '{dropbox_path}' (API path: '{path_to_list}')")
    try:
        result = dbx.files_list_folder(path=path_to_list, limit=100)

        if not result.entries:
            print("  [Folder is empty or does not exist]")
            return

        print("  --- Contents ---")
        for entry in result.entries:
            if isinstance(entry, FolderMetadata):
                print(f"  [Folder] {entry.name}")
            elif isinstance(entry, FileMetadata):
                print(f"  [File]   {entry.name}")
            else:
                print(f"  [Other]  {entry.name}")
        print("  --------------")

        if result.has_more:
            print("  (Note: More items exist in this folder, pagination needed for full list)")

    except ApiError as e:
        if e.error.is_path() and e.error.get_path().is_not_found():
            print(f"  Error: Path '{dropbox_path}' not found on Dropbox.")
        else:
            print(f"  Dropbox API Error while listing path: {e}")
    except Exception as e:
        print(f"  An unexpected error occurred while listing path: {e}")

def verify_dropbox_upload(dbx: dropbox.Dropbox, local_file_path: str, dropbox_folder: str, base_filename: str, cleanup=True) -> bool:
    """
    Verifies if a file matching the pattern exists in Dropbox, matches local content,
    and optionally cleans it up.

    Args:
        dbx: Initialized Dropbox client.
        local_file_path: Path to the original local file for content comparison.
        dropbox_folder: The folder path on Dropbox where the file should be.
        base_filename: The original filename before timestamp (e.g., "output.xml").
        cleanup: If True, delete the file from Dropbox after successful verification.

    Returns:
        True if verification succeeds, False otherwise.
    """
    print(f"\nVerifying upload in Dropbox folder '{dropbox_folder}' for base file '{base_filename}'...")
    if not os.path.exists(local_file_path):
        print(f"  Verification Error: Local file '{local_file_path}' not found.")
        return False

    # Normalize folder path for listing (API expects "" for root)
    list_folder_path = dropbox_folder if dropbox_folder != "/" else ""
    if list_folder_path and not list_folder_path.startswith('/'):
         list_folder_path = '/' + list_folder_path

    # Prepare expected filename pattern
    base, ext = os.path.splitext(base_filename)
    expected_prefix = base + "_"
    expected_suffix = ext

    found_file_entry = None
    try:
        print(f"  Listing contents of '{dropbox_folder}'...")
        result = dbx.files_list_folder(path=list_folder_path)
        # Find the most recent matching file (if multiple exist)
        matching_files = []
        for entry in result.entries:
             # Check if it's a file and matches the naming pattern: base_YYYYMMDD_HHMMSS.ext
            if (isinstance(entry, FileMetadata) and
                entry.name.startswith(expected_prefix) and
                entry.name.endswith(expected_suffix) and
                len(entry.name) == len(expected_prefix) + 15 + len(expected_suffix)): # 15 = len(YYYYMMDD_HHMMSS)
                 # Basic check for timestamp digits - could be more robust
                 timestamp_part = entry.name[len(expected_prefix):-len(expected_suffix)]
                 if len(timestamp_part) == 15 and timestamp_part[:8].isdigit() and timestamp_part[9:].isdigit():
                     matching_files.append(entry)

        if not matching_files:
            print(f"  Verification Failed: No file matching pattern '{expected_prefix}*{expected_suffix}' found in '{dropbox_folder}'.")
            return False

        # Sort by name descending to get the latest timestamp first
        matching_files.sort(key=lambda x: x.name, reverse=True)
        found_file_entry = matching_files[0] # Verify the latest one
        print(f"  Found potential match: '{found_file_entry.name}' (Path: {found_file_entry.path_display})")

        # --- Content Verification ---
        print(f"  Downloading '{found_file_entry.name}' for content verification...")
        _, res = dbx.files_download(path=found_file_entry.path_display)
        downloaded_content = res.content

        print(f"  Reading local file '{local_file_path}'...")
        with open(local_file_path, 'rb') as f:
            local_content = f.read()

        if downloaded_content == local_content:
            print("  Verification Success: Content matches local file.")
            # --- Cleanup ---
            if cleanup:
                print(f"  Attempting to delete '{found_file_entry.name}' from Dropbox...")
                try:
                    dbx.files_delete_v2(path=found_file_entry.path_display)
                    print(f"  Successfully deleted '{found_file_entry.name}'.")
                except ApiError as e:
                    print(f"  Warning: Failed to delete verified file '{found_file_entry.name}': {e}")
                    # Decide if non-deletion should fail the verification (maybe not)
            else:
                 print("  Cleanup skipped.")
            return True
        else:
            print("  Verification Failed: Downloaded content does not match local file content.")
            # print(f"  Local size: {len(local_content)}, Downloaded size: {len(downloaded_content)}") # Debugging aid
            return False

    except ApiError as e:
        if e.error.is_path() and e.error.get_path().is_not_found():
             print(f"  Verification Error: Dropbox folder '{dropbox_folder}' not found.")
        else:
             print(f"  Verification Error: Dropbox API error - {e}")
        return False
    except Exception as e:
        print(f"  Verification Error: An unexpected error occurred - {e}")
        return False


# --- Integrated Test and Verification Function ---
def run_upload_test_and_verify(dbx: dropbox.Dropbox):
    """
    Creates a temporary file, uploads it using upload_file_to_dropbox,
    then verifies the upload using verify_dropbox_upload, and cleans up.
    """
    # --- Test Configuration ---
    test_local_filename = f"temp_verify_test_{uuid.uuid4()}.txt"

    test_dropbox_folder = "/PythonScriptTests" # Must exist in dropbox otherwise fail
    test_base_filename = "verify_output.txt" 
    test_dropbox_dest = os.path.join(test_dropbox_folder, test_base_filename).replace("\\", "/") 
    test_content = f"Verification test content. Timestamp: {datetime.datetime.now(datetime.timezone.utc).isoformat()}."

    print("*" * 30)
    print("Running Upload Test and Verification")
    print(f"Local test file: {test_local_filename}")
    print(f"Target Dropbox Folder: {test_dropbox_folder}")
    print(f"Target Base Filename: {test_base_filename}")
    print("*" * 30)

    overall_success = False
    uploaded_path = None

    try:
        # --- Setup ---
        print("\nCreating temporary local file...")
        with open(test_local_filename, 'w') as f:
            f.write(test_content)
        print(f"Local file '{test_local_filename}' created.")

        # --- Execute Upload ---
        uploaded_path = upload_file_to_dropbox(dbx, test_local_filename, test_dropbox_dest)
        print(f"Upload function completed. Reported path: {uploaded_path}")

        # --- Verify Upload ---
        verification_result = verify_dropbox_upload(
            dbx=dbx,
            local_file_path=test_local_filename,
            dropbox_folder=test_dropbox_folder,
            base_filename=test_base_filename,
            cleanup=True 
        )

        if verification_result:
            print("\nOverall Result: TEST PASSED")
            overall_success = True
        else:
            print("\nOverall Result: TEST FAILED")
            overall_success = False

    except (FileNotFoundError, ValueError, AuthError, ApiError) as e:
        print(f"\nTest Execution Error: {e}")
        overall_success = False
    except Exception as e:
        print(f"\nAn unexpected error occurred during the test run: {e}")
        overall_success = False
    finally:
        # --- Local Teardown ---
        if os.path.exists(test_local_filename):
            try:
                os.remove(test_local_filename)
                print(f"\nCleaned up temporary local file: {test_local_filename}")
            except Exception as e:
                print(f"Error cleaning up local file {test_local_filename}: {e}")
        print("*" * 30)
        print("Upload Test and Verification Finished")
        print("*" * 30)
        return overall_success


# --- Main Execution ---
if __name__ == "__main__":
    try:
        dbx_client = get_dropbox_client()

        # test_passed = run_upload_test_and_verify(dbx_client)

        # print(f"\nFinal Status from main block: {'SUCCESS' if test_passed else 'FAILURE'}")

        # print("\nListing test folder after verification (should be empty if cleanup=True)...")
        list_dropbox_path(dbx_client, "/")

        # upload_file_to_dropbox(dbx_client,"bla.xml", "/Reports/stock/stock.xml")


    except (ValueError, AuthError) as e:
        print(f"Execution failed due to configuration or authentication issue: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in main execution: {e}")