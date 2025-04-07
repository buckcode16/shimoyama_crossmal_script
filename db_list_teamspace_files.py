import dropbox
from dropbox.exceptions import AuthError, ApiError, BadInputError
import os
from dotenv import load_dotenv
from dropbox.files import WriteMode, FolderMetadata, FileMetadata
import datetime
import sys
import json 
# --- Configuration and Setup ---
current_dt = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))) # JST
print(f"Script started at: {current_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
print(f"Location context: Chuo City, Tokyo, Japan")

load_dotenv()

DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")

# --- Load Namespace and User IDs ---
TEAM_SPACE_NAMESPACE_ID = os.getenv("TEAM_SPACE_NAMESPACE_ID") 
TARGET_USER_MEMBER_ID = os.getenv("TARGET_USER_MEMBER_ID") 

print("\n--- Configuration ---")
print(f"Using Team Space Namespace ID: {TEAM_SPACE_NAMESPACE_ID}")
print(f"Operating as User: {TARGET_USER_MEMBER_ID}")
print("-" * 20)

# --- Validate Configuration ---
if not all([DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN]): sys.exit("Error: Dropbox credentials not found.")
if not TEAM_SPACE_NAMESPACE_ID: sys.exit("Error: TEAM_SPACE_NAMESPACE_ID not set.")
if not TARGET_USER_MEMBER_ID or not TARGET_USER_MEMBER_ID.startswith("dbmid:"): sys.exit(f"Error: Invalid TARGET_USER_MEMBER_ID format: '{TARGET_USER_MEMBER_ID}'. Must start with 'dbmid:'.")

# --- Initialize Client with COMBINED Manual Headers ---
dbx_client = None 
try:
    print(f"Initializing Dropbox client with manual headers for Namespace ID {TEAM_SPACE_NAMESPACE_ID} and User {TARGET_USER_MEMBER_ID}")

    # --- Construct Combined Headers ---
    # Path Root Header Value (as JSON string)
    # Restore the ".tag" key as required by the API error
    path_root_header_value = {
        ".tag": "namespace_id", 
        "namespace_id": TEAM_SPACE_NAMESPACE_ID
    }
    path_root_json_string = json.dumps(path_root_header_value)

    # Select User Header Value (the dbmid string itself)
    select_user_header_value = TARGET_USER_MEMBER_ID

    # Combined Headers Dictionary
    custom_headers = {
        "Dropbox-API-Path-Root": path_root_json_string, 
        "Dropbox-API-Select-User": select_user_header_value
    }
    print(f"Corrected Custom Headers being sent: {custom_headers}")
    # --------------------------------

    # Initialize the standard client, passing the combined headers, not passing path_root
    dbx_client = dropbox.Dropbox(
        app_key=DROPBOX_APP_KEY,
        app_secret=DROPBOX_APP_SECRET,
        oauth2_refresh_token=DROPBOX_REFRESH_TOKEN,
        headers=custom_headers # <--- Pass combined headers
    )

    # Verify the client works - this call now requires BOTH headers when using team token
    print("Verifying client access to Team Space root (using combined headers)...")
    dbx_client.files_list_folder(path="", limit=1) # Should succeed now
    print("Successfully initialized and verified client with combined headers.")

# --- Exception Handling for Initialization ---
except BadInputError as e:
     # If this specific error still happens, something is fundamentally wrong
     if 'Dropbox-API-Select-User' in str(e) or 'single Dropbox account' in str(e):
         print(f"\nERROR during verification: API still rejected the call despite providing both headers.")
         print(f"   Please double-check: ")
         print(f"     - Correctness of Namespace ID: {TEAM_SPACE_NAMESPACE_ID}")
         print(f"     - Correctness of Member ID: {TARGET_USER_MEMBER_ID}")
         print(f"     - Token Validity & Permissions (Scopes in App Console)")
         print(f"   Original Error: {e}")
     else:
        print(f"Dropbox API Bad Input error during init/verify: {e}")
     sys.exit(1)
except ApiError as e: print(f"API Error during init/verify: {e}"); sys.exit(1)
except AuthError as e: print(f"Auth Error during init/verify: {e}"); sys.exit(1)
# Catch the original TypeError just in case something unexpected happens with headers arg
except TypeError as e: print(f"TypeError during init (check SDK version?): {e}"); sys.exit(1)
except Exception as e: print(f"Unexpected error during init: {e}"); sys.exit(1)


# --- Perform Operations using the Client with Combined Headers ---
if dbx_client:
    print(f"\n--- Operating within Team Space (Namespace ID: {TEAM_SPACE_NAMESPACE_ID}) as User {TARGET_USER_MEMBER_ID} ---")
    try:
        # Use dbx_client for all operations. It has the headers set.
        print("Listing contents of the Team Space root ('') ...")
        list_folder_result = dbx_client.files_list_folder(path="") # Path is relative to Team Space root defined in header

        print("\nContents:")
        if not list_folder_result.entries:
            print("  [Team Space root is empty or contains no items the token/user can see]")

        for entry in list_folder_result.entries:
            entry_type = "Folder" if isinstance(entry, FolderMetadata) else "File"
            print(f"  - {entry.name} ({entry_type})")

        # Handle pagination
        while list_folder_result.has_more:
             print("  Fetching more entries...")
             list_folder_result = dbx_client.files_list_folder_continue(list_folder_result.cursor)
             for entry in list_folder_result.entries:
                 entry_type = "Folder" if isinstance(entry, FolderMetadata) else "File"
                 print(f"  - {entry.name} ({entry_type})")


        # --->>> ADD YOUR SPECIFIC FILE/FOLDER OPERATIONS HERE <<<---
        # Use the 'dbx_client' object

    # --- Exception Handling for Operations ---
    except ApiError as e: print(f"API Error during operation: {e}")
    except Exception as e: print(f"Unexpected error during operation: {e}")

else:
    print("\nCould not initialize the Dropbox client. Skipping operations.")

print("\n--- Script Finished ---")