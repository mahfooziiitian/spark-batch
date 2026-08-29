"""OAuth2 authentication helper for REST API data sources.

Supports three OAuth2 flows:
    - Client Credentials (machine-to-machine, most common for APIs)
    - Resource Owner Password (username/password → token)
    - Bearer Token (pre-obtained token, no refresh)

The helper is designed to be pickle-serializable: token state is fetched lazily
inside worker methods, not at construction time.

Usage from data source options:
    .option("auth", "oauth2")
    .option("oauth.tokenUrl", "https://auth.example.com/oauth/token")
    .option("oauth.clientId", "my-client-id")
    .option("oauth.clientSecret", "my-client-secret")
    .option("oauth.scope", "read write")             # optional
    .option("oauth.grantType", "client_credentials") # default
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass
class OAuth2Config:
    """OAuth2 configuration extracted from data source options.

    All fields are simple types for pickle serialization.
    """

    token_url: str = ""
    client_id: str = ""
    client_secret: str = ""
    username: str = ""
    password: str = ""
    scope: str = ""
    grant_type: str = "client_credentials"
    bearer_token: str = ""
    extra_params: dict = field(default_factory=dict)

    @classmethod
    def from_options(cls, options: Mapping[str, str]) -> OAuth2Config | None:
        """Extract OAuth2 config from data source options.

        Returns None if auth is not set to "oauth2".
        """
        auth_type = (options.get("auth") or options.get("Auth") or "").lower()
        if auth_type != "oauth2":
            return None

        return cls(
            token_url=options.get("oauth.tokenUrl") or options.get("oauth.tokenurl") or "",
            client_id=options.get("oauth.clientId") or options.get("oauth.clientid") or "",
            client_secret=(
                options.get("oauth.clientSecret") or options.get("oauth.clientsecret") or ""
            ),
            username=options.get("oauth.username") or "",
            password=options.get("oauth.password") or "",
            scope=options.get("oauth.scope") or "",
            grant_type=(
                options.get("oauth.grantType")
                or options.get("oauth.granttype")
                or "client_credentials"
            ),
            bearer_token=(
                options.get("oauth.bearerToken") or options.get("oauth.bearertoken") or ""
            ),
            extra_params=_extract_prefixed(options, "oauth.param."),
        )

    def fetch_token(self) -> str:
        """Fetch an access token from the token endpoint.

        This method is called inside read()/write() on workers — it imports
        requests locally to stay pickle-safe.
        """
        if self.bearer_token:
            return self.bearer_token

        if not self.token_url:
            raise ValueError(
                "oauth.tokenUrl is required when auth=oauth2 and no oauth.bearerToken is provided"
            )

        import requests

        payload: dict[str, str] = {"grant_type": self.grant_type}

        if self.grant_type == "client_credentials":
            payload["client_id"] = self.client_id
            payload["client_secret"] = self.client_secret
        elif self.grant_type == "password":
            payload["client_id"] = self.client_id
            payload["client_secret"] = self.client_secret
            payload["username"] = self.username
            payload["password"] = self.password
        else:
            payload["client_id"] = self.client_id
            payload["client_secret"] = self.client_secret

        if self.scope:
            payload["scope"] = self.scope

        payload.update(self.extra_params)

        response = requests.post(
            self.token_url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        response.raise_for_status()
        token_data = response.json()

        access_token = token_data.get("access_token")
        if not access_token:
            raise ValueError(f"OAuth2 token response missing 'access_token': {token_data}")

        return access_token

    def apply_to_headers(self, headers: dict) -> dict:
        """Fetch a token and add Authorization header. Returns updated headers."""
        token = self.fetch_token()
        headers = {**headers, "Authorization": f"Bearer {token}"}
        return headers


def build_auth_headers(options: dict, headers: dict) -> tuple[dict, OAuth2Config | None]:
    """Build request headers with authentication applied.

    Checks for OAuth2 config first, falls back to API key auth.
    Returns (updated_headers, oauth_config_or_none).
    """
    oauth_config = OAuth2Config.from_options(options)

    if oauth_config is not None:
        return headers, oauth_config

    # Fall back to API key auth
    api_key = options.get("apiKey") or options.get("apikey")
    if api_key:
        header_name = options.get("apiKeyHeader") or options.get("apikeyheader") or "X-API-Key"
        headers[header_name] = api_key

    return headers, None


def _extract_prefixed(options: Mapping[str, str], prefix: str) -> dict[str, str]:
    """Extract options with a prefix, stripping it from keys."""
    result = {}
    for key, value in options.items():
        if key.lower().startswith(prefix.lower()):
            result[key[len(prefix) :]] = value
    return result
