from authlib.integrations.requests_client import OAuth2Session

client = OAuth2Session(
    client_id="Ov23liu6RQn7dtNHiP4g",
    client_secret="f686b228efa7dc7c4a705d5d0380c9821c770a2a",
    redirect_uri="https://yourapp.com/callback",
)
# Redirect user to authorization URL
uri, state = client.create_authorization_url("https://provider.com/authorize")

# After user returns with ?code=..., exchange for token
token = client.fetch_token(
    "https://provider.com/token",
    authorization_response="https://yourapp.com/callback?code=abc123",
)
