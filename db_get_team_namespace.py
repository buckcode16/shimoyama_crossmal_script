import dropbox
from dropbox.dropbox_client import DropboxTeam
from dropbox.exceptions import AuthError, ApiError
import os
from dotenv import load_dotenv
import sys

# --- Configuration ---
load_dotenv()
DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN") # Your team token

print("--- List Team Namespaces to find Team Space Root ID ---")

if not all([DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN]):
    print("\nError: Dropbox credentials not found.")
    sys.exit(1)

# --- Initialize Client ---
dbx_base = None
try:
    print("\nInitializing DropboxTeam client...")
    dbx_base = DropboxTeam( # Use DropboxTeam
        app_key=DROPBOX_APP_KEY,
        app_secret=DROPBOX_APP_SECRET,
        oauth2_refresh_token=DROPBOX_REFRESH_TOKEN
    )
    dbx_base.team_get_info() # Verify token/connection
    print("DropboxTeam client initialized successfully.")

except Exception as e:
    print(f"\nError during initialization (check token/permissions): {e}")
    sys.exit(1)

# --- List Namespaces ---
try:
    print("\nAttempting to list team namespaces...")
    # This endpoint lists namespaces including team folders, shared folders, and the team root
    namespaces_result = dbx_base.team_namespaces_list()

    print("\n--- Found Namespaces ---")
    possible_root_id = None
    for ns in namespaces_result.namespaces:
        print(f"  Name: '{ns.name}', Namespace ID: {ns.namespace_id}, Type: {ns.namespace_type}")
        # Heuristic: The team space root often has the team's name
        # or might be identifiable by type if the SDK provides clearer type info.
        # You may need to identify it based on the 'Name'.
        # Store the first one found as a candidate, or look for specific indicators.
        # You might need to compare this list with what you see in the web UI.
        # For now, just print all of them. You'll need to visually inspect.

    # Basic pagination example if needed
    while namespaces_result.has_more:
         print("  Fetching more namespaces...")
         namespaces_result = dbx_base.team_namespaces_list_continue(namespaces_result.cursor)
         for ns in namespaces_result.namespaces:
             print(f"  Name: '{ns.name}', Namespace ID: {ns.namespace_id}, Type: {ns.namespace_type}")

    print("----------------------")
    print("Examine the list above. The 'Team Space Root' namespace might:")
    print(" - Have a name similar to your overall Team Name.")
    print(" - Be listed with a specific 'namespace_type' (consult SDK docs if needed).")
    print("Identify the correct Namespace ID from the list.")

except ApiError as e:
    print(f"\nAPI Error listing namespaces.")
    print(f"Ensure token has Admin rights and 'team_data.member' scope.")
    print(f"Details: {e}")
except Exception as e:
    print(f"\nAn unexpected error occurred: {e}")