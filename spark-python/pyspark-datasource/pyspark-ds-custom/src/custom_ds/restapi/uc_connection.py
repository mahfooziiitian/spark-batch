"""Unity Catalog HTTP connection helper for REST API data sources.

Unity Catalog HTTP connections are securable objects that store endpoint and
credential information for external HTTP services. They support multiple
authentication methods:

- **Bearer Token**: Simple token-based auth (``bearer_token``)
- **OAuth Machine-to-Machine (M2M)**: Server-to-server via ``client_id``,
  ``client_secret``, ``token_endpoint``, and ``oauth_scope``
- **OAuth User-to-Machine**: Per-user or shared identity flows (managed by
  Databricks, not directly handled by this module)

When running on Databricks (DBR 15.4+ for connections, DBR 18.1+ for
credential injection into Python Data Sources), users reference a UC HTTP
connection via the ``databricks.connection`` option. The Spark driver
automatically resolves the connection and injects keys such as ``host``,
``base_path``, ``bearer_token``, ``port``, ``client_id``, ``client_secret``,
``token_endpoint``, and ``oauth_scope`` into the data source options dict.

This module provides a helper to resolve those injected values into a concrete
base URL and auth headers, so the REST API sources can transparently support
both UC connections (on Databricks) and explicit options (locally).

Usage from data source options::

    # On Databricks -- UC injects credentials automatically
    .option("databricks.connection", "my_http_connection")

    # Locally (bearer token) -- pass the same keys explicitly
    .option("uc.host", "https://api.example.com")
    .option("uc.basePath", "/v2")
    .option("uc.bearerToken", "my-token")

    # Locally (OAuth M2M) -- pass client credentials
    .option("uc.host", "https://api.example.com")
    .option("uc.clientId", "my-client-id")
    .option("uc.clientSecret", "my-client-secret")
    .option("uc.tokenEndpoint", "https://auth.example.com/token")
    .option("uc.oauthScope", "read write")

Priority (highest wins):
    1. UC-injected keys (from ``databricks.connection``, cannot be overridden)
    2. Explicit ``uc.*`` options (for local testing)
    3. Standard ``url``/auth options (existing behavior)

Reference:
    https://docs.databricks.com/aws/en/query-federation/http
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass
class UCConnectionConfig:
    """Resolved Unity Catalog HTTP connection parameters.

    Supports both **Bearer Token** and **OAuth Machine-to-Machine** (M2M)
    connection types. All fields are simple types for pickle serialization
    across Spark workers.

    On Databricks, the ``CREATE CONNECTION`` SQL command defines the
    connection::

        -- Bearer Token
        CREATE CONNECTION my_api TYPE HTTP
        OPTIONS (
            host 'https://api.example.com',
            base_path '/v2',
            bearer_token secret('scope', 'key')
        );

        -- OAuth M2M
        CREATE CONNECTION my_api TYPE HTTP
        OPTIONS (
            host 'https://api.example.com',
            base_path '/v2',
            client_id 'my-client',
            client_secret secret('scope', 'key'),
            token_endpoint 'https://auth.example.com/token',
            oauth_scope 'read write'
        );
    """

    host: str
    base_path: str = ""
    bearer_token: str = ""
    port: str = ""
    # OAuth M2M fields (injected when connection uses OAuth M2M auth type)
    client_id: str = ""
    client_secret: str = ""
    token_endpoint: str = ""
    oauth_scope: str = ""
    # Connection metadata
    connection_name: str = ""
    auth_type: str = ""  # "bearer_token", "oauth_m2m", or ""

    @classmethod
    def from_options(cls, options: Mapping[str, str]) -> UCConnectionConfig | None:
        """Extract UC HTTP connection config from data source options.

        On Databricks, the driver injects keys like ``host``, ``base_path``,
        ``bearer_token`` (for bearer auth) or ``client_id``, ``client_secret``,
        ``token_endpoint`` (for OAuth M2M) when ``databricks.connection`` is set.

        For local testing, users can pass ``uc.host``, ``uc.basePath``,
        ``uc.bearerToken``, ``uc.clientId``, ``uc.clientSecret``, etc.

        Returns None if no UC connection is configured.
        """
        connection_name = (
            options.get("databricks.connection") or options.get("Databricks.Connection") or ""
        )

        # --- UC-injected keys (set automatically by Databricks driver) ---
        host = options.get("host") or options.get("Host") or ""
        base_path = options.get("base_path") or options.get("Base_Path") or ""
        bearer_token = options.get("bearer_token") or options.get("Bearer_Token") or ""
        port = options.get("port") or options.get("Port") or ""
        client_id = options.get("client_id") or options.get("Client_Id") or ""
        client_secret = options.get("client_secret") or options.get("Client_Secret") or ""
        token_endpoint = options.get("token_endpoint") or options.get("Token_Endpoint") or ""
        oauth_scope = options.get("oauth_scope") or options.get("Oauth_Scope") or ""

        # --- Explicit uc.* options (for local testing without Databricks) ---
        uc_host = options.get("uc.host") or options.get("uc.Host") or ""
        uc_base_path = options.get("uc.basePath") or options.get("uc.basepath") or ""
        uc_bearer_token = options.get("uc.bearerToken") or options.get("uc.bearertoken") or ""
        uc_port = options.get("uc.port") or options.get("uc.Port") or ""
        uc_client_id = options.get("uc.clientId") or options.get("uc.clientid") or ""
        uc_client_secret = options.get("uc.clientSecret") or options.get("uc.clientsecret") or ""
        uc_token_endpoint = options.get("uc.tokenEndpoint") or options.get("uc.tokenendpoint") or ""
        uc_oauth_scope = options.get("uc.oauthScope") or options.get("uc.oauthscope") or ""

        # UC-injected values take precedence over uc.* options
        resolved_host = host or uc_host
        resolved_base_path = base_path or uc_base_path
        resolved_bearer_token = bearer_token or uc_bearer_token
        resolved_port = port or uc_port
        resolved_client_id = client_id or uc_client_id
        resolved_client_secret = client_secret or uc_client_secret
        resolved_token_endpoint = token_endpoint or uc_token_endpoint
        resolved_oauth_scope = oauth_scope or uc_oauth_scope

        if not resolved_host and not connection_name:
            return None

        # Determine auth type
        auth_type = ""
        if resolved_bearer_token:
            auth_type = "bearer_token"
        elif resolved_client_id and resolved_client_secret:
            auth_type = "oauth_m2m"

        return cls(
            host=resolved_host,
            base_path=resolved_base_path,
            bearer_token=resolved_bearer_token,
            port=resolved_port,
            client_id=resolved_client_id,
            client_secret=resolved_client_secret,
            token_endpoint=resolved_token_endpoint,
            oauth_scope=resolved_oauth_scope,
            connection_name=connection_name,
            auth_type=auth_type,
        )

    def build_base_url(self) -> str:
        """Construct the base URL from host, port, and base_path.

        Follows the same URL construction pattern as the Databricks HTTP
        connection proxy: ``{host}{base_path}``

        Examples:
            host=https://api.example.com, base_path=/v2 -> https://api.example.com/v2
            host=https://api.example.com, port=8443     -> https://api.example.com:8443
        """
        url = self.host.rstrip("/")

        if self.port:
            from urllib.parse import urlparse, urlunparse

            parsed = urlparse(url)
            url = urlunparse(parsed._replace(netloc=f"{parsed.hostname}:{self.port}"))

        if self.base_path:
            url = f"{url}/{self.base_path.strip('/')}"

        return url

    def resolve_url(self, user_url: str | None, path: str = "") -> str:
        """Resolve the final request URL.

        If a user-provided URL is given, it takes precedence (allows overriding
        the full endpoint). Otherwise, builds from host + base_path + path,
        matching the Databricks proxy pattern: ``{host}{base_path}{sub-path}``.
        """
        if user_url:
            return user_url

        base = self.build_base_url()
        if path:
            return f"{base}/{path.lstrip('/')}"
        return base

    def fetch_oauth_token(self) -> str:
        """Fetch an OAuth2 access token using client credentials (M2M) flow.

        This method is called on Spark workers, so it imports ``urllib``
        inside the method body to avoid serialization issues.

        Returns:
            The access token string.

        Raises:
            RuntimeError: If the token request fails.
            ValueError: If token_endpoint or client credentials are missing.
        """
        if not self.token_endpoint:
            raise ValueError("token_endpoint is required for OAuth M2M authentication")
        if not self.client_id or not self.client_secret:
            raise ValueError("client_id and client_secret are required for OAuth M2M")

        import json
        import urllib.error
        import urllib.parse
        import urllib.request

        data = urllib.parse.urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                **({"scope": self.oauth_scope} if self.oauth_scope else {}),
            }
        ).encode()

        req = urllib.request.Request(  # noqa: S310
            self.token_endpoint,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
                token_data = json.loads(resp.read().decode())
        except (urllib.error.URLError, TimeoutError) as e:
            raise RuntimeError(f"OAuth M2M token request failed: {e}") from e

        access_token = token_data.get("access_token")
        if not access_token:
            raise RuntimeError(f"No access_token in OAuth response: {token_data}")

        return access_token

    def apply_auth_headers(self, headers: dict) -> dict:
        """Add authentication headers based on the connection auth type.

        For bearer token connections, adds ``Authorization: Bearer <token>``.
        For OAuth M2M connections, fetches a fresh token via client credentials
        flow and adds it as a bearer token.

        Returns a new dict (does not mutate the input).
        """
        if self.auth_type == "bearer_token" and self.bearer_token:
            return {**headers, "Authorization": f"Bearer {self.bearer_token}"}
        if self.auth_type == "oauth_m2m":
            token = self.fetch_oauth_token()
            return {**headers, "Authorization": f"Bearer {token}"}
        return headers
