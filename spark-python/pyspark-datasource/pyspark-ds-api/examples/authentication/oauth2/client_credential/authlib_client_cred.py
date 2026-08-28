import os

from authlib.integrations.requests_client import OAuth2Session
from dotenv import load_dotenv

load_dotenv()

# Configuration
client_id = os.getenv("GITHUB_CLIENT_ID")
client_secret = os.getenv("GITHUB_CLIENT_SECRET")
token_url = "https://authorization-server.com/oauth2/token"
api_url = "https://api.server.com/protected/resource"

# Create OAuth2 session
client = OAuth2Session(client_id, client_secret, scope="your_scope")

# Fetch access token
token = client.fetch_token(token_url=token_url)

# Use token to make authorized API call
response = client.get(api_url)
print(response.status_code)
print(response.json())
