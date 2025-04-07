import dropbox
from dropbox.dropbox_client import DropboxTeam # Use the Team client
from dropbox.exceptions import AuthError, ApiError
import os
from dotenv import load_dotenv
import sys # To exit cleanly after finding the ID

# --- Configuration ---
load_dotenv()
DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN") # Your team token

print("--- Find My Dropbox Team Member ID ---")

# --- Get Admin Email ---
admin_email_to_find = input("Enter your Dropbox Admin email address: ").strip().lower()
if not admin_email_to_find:
    print("Error: Email address cannot be empty.")
    sys.exit(1)

# --- Validate Credentials ---
if not all([DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN]):
    print("\nError: Dropbox credentials (APP_KEY, APP_SECRET, REFRESH_TOKEN) not found in environment variables (.env file).")
    sys.exit(1)

# --- Initialize Client ---
dbx_base = None
try:
    print("\nInitializing DropboxTeam client...")
    # Initialize directly with DropboxTeam using the team token
    dbx_base = DropboxTeam(
        app_key=DROPBOX_APP_KEY,
        app_secret=DROPBOX_APP_SECRET,
        oauth2_refresh_token=DROPBOX_REFRESH_TOKEN
    )
    # Quick check to see if token is fundamentally valid (optional but good)
    # Note: This call itself might require admin permissions captured by the token
    dbx_base.team_get_info()
    print("DropboxTeam client initialized successfully.")

except AuthError as e:
     print(f"\nAuthentication Error: Failed to initialize client. Check your Refresh Token.")
     print(f"Details: {e}")
     sys.exit(1)
except ApiError as e:
     print(f"\nAPI Error: Failed during initial client check. Check token permissions/validity.")
     print(f"Details: {e}")
     sys.exit(1)
except Exception as e:
    print(f"\nAn unexpected error occurred during initialization: {e}")
    sys.exit(1)


# --- Find Member ID ---
found_id = None
try:
    print(f"\nAttempting to list team members to find email: {admin_email_to_find}...")
    # Call the team-level endpoint directly using the DropboxTeam client
    members_list_result = dbx_base.team_members_list()

    while True: # Loop for handling members list and pagination
        print(f"  Checking {len(members_list_result.members)} members in current batch...")
        for member in members_list_result.members:
            profile = member.profile
            # Compare emails case-insensitively
            if profile.email.lower() == admin_email_to_find:
                found_id = profile.team_member_id
                print("\n" + "="*40)
                print(f"  SUCCESS: Found matching email!")
                print(f"  Email:       {profile.email}")
                print(f"  Member ID:   {found_id}")
                print(f"  Status:      {profile.status}")
                print("="*40)
                break # Exit inner loop once found

        if found_id:
            break # Exit outer loop if found

        # Handle pagination
        if members_list_result.has_more:
             print("  Fetching next page of members...")
             members_list_result = dbx_base.team_members_list_continue(members_list_result.cursor)
        else:
             break # No more members to check

except ApiError as e:
    print(f"\nAPI Error: Failed to list team members.")
    print(f"Check if your token has 'members.read' permission.")
    print(f"Details: {e}")
    sys.exit(1)
except Exception as e:
    print(f"\nAn unexpected error occurred while listing members: {e}")
    sys.exit(1)

# --- Final Result ---
if not found_id:
    print("\n--------------------")
    print(f"Error: Could not find a team member with the email address: {admin_email_to_find}")
    print("Please ensure the email address is correct and the account is part of this team.")
    print("--------------------")
    sys.exit(1)
else:
    print("\nScript finished successfully.")
    # The ID was already printed above
    sys.exit(0)