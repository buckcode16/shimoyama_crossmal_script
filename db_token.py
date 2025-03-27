#!/usr/bin/env python3
import dropbox
import os
from dotenv import load_dotenv
load_dotenv()

DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")

auth_flow = dropbox.DropboxOAuth2FlowNoRedirect(
    DROPBOX_APP_KEY,
    DROPBOX_APP_SECRET,
    token_access_type='offline'
)

# Step 1: Get user to authorize the app
authorize_url = auth_flow.start()
print("1. Go to:", authorize_url)
print("2. Click 'Allow'")
print("3. Copy the authorization code shown.")

auth_code = input("Enter the authorization code here: ").strip()

# Step 2: Exchange code for access and refresh tokens
oauth_result = auth_flow.finish(auth_code)

print("\n✅ SUCCESS! Save these tokens securely:")
print("Access Token:", oauth_result.access_token)
print("Refresh Token:", oauth_result.refresh_token)
print("Expires At:", oauth_result.expires_at)
