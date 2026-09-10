"""Helpers for generating Databricks dashboard embed tokens and URLs."""

from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass
from typing import Any, cast
from urllib import error, parse, request

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EmbedConfig:
    """Configuration required for Databricks dashboard embedding.

    Args:
        instance_url: Databricks workspace base URL.
        workspace_id: Databricks workspace ID used in embed URLs.
        dashboard_id: Published Lakeview dashboard identifier.
        service_principal_id: OAuth client ID for the embedding service principal.
        service_principal_secret: OAuth client secret for the embedding service principal.
    """

    instance_url: str
    workspace_id: str
    dashboard_id: str
    service_principal_id: str
    service_principal_secret: str


def _http_request(
    url: str,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: str | None = None,
) -> dict[str, Any]:
    """Send an HTTP request and parse the JSON response."""
    if not url.startswith(("https://", "http://")):
        raise ValueError(f"Refusing to request non-HTTP(S) URL: {url}")

    request_headers = headers or {}
    payload = body.encode("utf-8") if body is not None else None
    req = request.Request(url, method=method, headers=request_headers, data=payload)

    try:
        with request.urlopen(req) as response:  # scheme validated above  # nosec B310
            response_body = response.read().decode("utf-8")
            parsed_response = json.loads(response_body)
            if not isinstance(parsed_response, dict):
                raise RuntimeError(f"Expected a JSON object from {url}, received {type(parsed_response).__name__}.")
            return cast("dict[str, Any]", parsed_response)
    except error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} calling {url}: {error_body}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Failed to call {url}: {exc.reason}") from exc


def get_scoped_token(config: EmbedConfig, external_viewer_id: str, external_value: str = "") -> str:
    """Exchange service principal credentials for a user-scoped embed token.

    Args:
        config: Databricks embedding configuration.
        external_viewer_id: Stable external viewer identifier used for audit logging.
        external_value: Optional value surfaced to dashboard queries as
            ``__aibi_external_value``.

    Returns:
        A scoped OAuth token for embedding the configured dashboard.

    Raises:
        RuntimeError: If any OAuth exchange request fails.
    """
    basic_auth = base64.b64encode(f"{config.service_principal_id}:{config.service_principal_secret}".encode()).decode("utf-8")

    logger.info("Requesting all-apis token for dashboard %s", config.dashboard_id)
    oidc_token_response = _http_request(
        f"{config.instance_url.rstrip('/')}/oidc/v1/token",
        method="POST",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {basic_auth}",
        },
        body=parse.urlencode({"grant_type": "client_credentials", "scope": "all-apis"}),
    )
    oidc_token = str(oidc_token_response["access_token"])

    logger.info("Requesting tokeninfo for external viewer %s", external_viewer_id)
    token_info_params = parse.urlencode(
        {
            "external_viewer_id": external_viewer_id,
            "external_value": external_value,
        }
    )
    token_info = _http_request(
        (
            f"{config.instance_url.rstrip('/')}/api/2.0/lakeview/dashboards/"
            f"{config.dashboard_id}/published/tokeninfo?{token_info_params}"
        ),
        headers={"Authorization": f"Bearer {oidc_token}"},
    )

    scoped_token_params = dict(token_info)
    authorization_details = scoped_token_params.pop("authorization_details", None)
    scoped_token_params["grant_type"] = "client_credentials"
    if authorization_details is not None:
        scoped_token_params["authorization_details"] = json.dumps(authorization_details)

    logger.info("Requesting scoped embed token for dashboard %s", config.dashboard_id)
    scoped_token_response = _http_request(
        f"{config.instance_url.rstrip('/')}/oidc/v1/token",
        method="POST",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {basic_auth}",
        },
        body=parse.urlencode(scoped_token_params),
    )
    return str(scoped_token_response["access_token"])


def get_embed_url(instance_url: str, workspace_id: str, dashboard_id: str) -> str:
    """Construct the full-dashboard embed URL.

    Args:
        instance_url: Databricks workspace base URL.
        workspace_id: Databricks workspace ID.
        dashboard_id: Published Lakeview dashboard identifier.

    Returns:
        The URL for embedding the full dashboard.
    """
    base_url = instance_url.rstrip("/")
    query_string = parse.urlencode({"o": workspace_id})
    return f"{base_url}/embed/dashboardsv3/{dashboard_id}?{query_string}"


def get_widget_embed_url(
    instance_url: str,
    workspace_id: str,
    dashboard_id: str,
    page_name: str,
    widget_name: str,
) -> str:
    """Construct the single-widget embed URL.

    Args:
        instance_url: Databricks workspace base URL.
        workspace_id: Databricks workspace ID.
        dashboard_id: Published Lakeview dashboard identifier.
        page_name: Page name containing the widget.
        widget_name: Widget name to render fullscreen.

    Returns:
        The URL for embedding a single dashboard widget.
    """
    fullscreen_widget = parse.quote(f"{page_name}~{widget_name}", safe="~")
    return f"{get_embed_url(instance_url, workspace_id, dashboard_id)}&fullscreenWidget={fullscreen_widget}"
