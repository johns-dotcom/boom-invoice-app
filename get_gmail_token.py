"""
Run this ONCE locally to get your Gmail OAuth refresh token.
Usage:
  1. Install: pip install google-auth-oauthlib
  2. Put your downloaded client_secrets.json in this folder
  3. Run: python get_gmail_token.py
  4. A browser window will open — sign in with the Gmail account
     you want the app to send FROM
  5. Copy the printed credentials into Railway env vars
"""

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

flow = InstalledAppFlow.from_client_secrets_file("client_secrets.json", SCOPES)
creds = flow.run_local_server(port=0)

print("\n✅ Success! Add these to Railway environment variables:\n")
print(f"GMAIL_CLIENT_ID     = {creds.client_id}")
print(f"GMAIL_CLIENT_SECRET = {creds.client_secret}")
print(f"GMAIL_REFRESH_TOKEN = {creds.refresh_token}")
print(f"GMAIL_USER          = the Gmail address you just signed in with")
