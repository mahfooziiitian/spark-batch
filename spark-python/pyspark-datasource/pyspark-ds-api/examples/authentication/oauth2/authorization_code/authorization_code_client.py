"""Demo of the OAuth2 authorization-code flow using authlib.

Set GITHUB_CLIENT_ID / GITHUB_CLIENT_SECRET in your environment before
running this script (never hardcode real OAuth app credentials in source
control) — see authlib_client_cred.py for the equivalent client-credentials
flow using the same env vars.
"""

import os

from authlib.integrations.requests_client import OAuth2Session

client = OAuth2Session(
    client_id=os.environ["GITHUB_CLIENT_ID"],
    client_secret=os.environ["GITHUB_CLIENT_SECRET"],
    redirect_uri="https://yourapp.com/callback",
)
# Redirect user to authorization URL
uri, state = client.create_authorization_url("https://provider.com/authorize")

# After user returns with ?code=..., exchange for token
token = client.fetch_token(
    "https://provider.com/token",
    authorization_response="https://yourapp.com/callback?code=abc123",
)
