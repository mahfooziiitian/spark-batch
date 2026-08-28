"""Canonical authentication implementation for REST API sources.

This module is the single source of truth for turning an `authentication`
YAML config block into request headers/`requests`-auth objects. It is used
both by `rest_ds.util.request_builder.build_request_components` (the
function-based ingestion path used by the incremental runner) and by
`rest_ds.rest_api.APIClient` (the class-based path used by most examples),
which delegates to `get_auth_headers` instead of duplicating this logic.
"""

import base64
import datetime
import uuid

import jwt
import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from requests.auth import HTTPBasicAuth


def _generate_assertion(auth_config: dict) -> str:
    """Builds a signed JWT assertion (RFC 7523) from the certificate pair
    referenced in `auth_config`, used by the `oauth2_assertion` auth flow."""
    with open(auth_config["public_key_path"], "rb") as pub_file:
        public_key = pub_file.read()
    with open(auth_config["private_key_path"], "rb") as priv_file:
        private_key = priv_file.read()

    cert = x509.load_pem_x509_certificate(public_key)
    # x5t/kid thumbprint per RFC 7523 — not used as a security hash.
    fingerprint = cert.fingerprint(hashes.SHA1())  # nosec B303
    x5t = base64.urlsafe_b64encode(fingerprint).decode("utf-8")
    kid = fingerprint.hex()
    request_branch_identifier_key = auth_config.get("request_branch_identifier_key")
    request_branch_identifier_value = auth_config.get("request_branch_identifier_value")

    payload: dict = {
        "jti": str(uuid.uuid4()),
        "iat": datetime.datetime.utcnow(),
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=5),
        "aud": auth_config["aud"],
    }
    if request_branch_identifier_key:
        payload[request_branch_identifier_key] = request_branch_identifier_value

    headers = {"x5t": x5t, "kid": kid}
    return jwt.encode(payload, private_key, algorithm="RS256", headers=headers)


def _generate_bearer_token(auth: dict) -> str:
    """Exchanges a JWT assertion for a bearer access token, used by the
    `oauth2_assertion` auth flow."""
    assertion = _generate_assertion(auth)
    grant_type_key = auth.get("grant_type_key")
    grant_type_value = auth.get("grant_type_value")
    token_url = auth.get("tokenUrl")
    # timeout is always set via auth.get("timeout", 60) below.
    response = requests.post(  # nosec B113
        token_url,
        json={grant_type_key: grant_type_value, "assertion": assertion},
        headers=auth.get("headers", {}),
        timeout=auth.get("timeout", 60),
    )
    response.raise_for_status()
    return response.json().get("access_token")


def _auth_basic(auth: dict):
    return {}, HTTPBasicAuth(auth["username"], auth["password"])


def _auth_bearer(auth: dict):
    return {"Authorization": f"Bearer {auth['token']}"}, None


def _auth_apikey(auth: dict):
    if auth.get("in") == "header":
        # Support both the "name"/"value" and "api_key_name"/"api_key_value"
        # config key conventions used across different example configs.
        key_name = auth.get("name", auth.get("api_key_name"))
        key_value = auth.get("value", auth.get("api_key_value"))
        return {key_name: key_value}, None
    # "in": "query" — headers stay empty; caller adds the key to query params.
    return {}, None


def _auth_oauth2_assertion(auth: dict):
    token = _generate_bearer_token(auth)
    return {"Authorization": f"Bearer {token}"}, None


def _auth_oauth2_client_credentials(auth: dict):
    data = {
        "grant_type": "client_credentials",
        "client_id": auth["clientId"],
        "client_secret": auth["clientSecret"],
        "scope": auth.get("scope", ""),
    }
    token_resp = requests.post(
        url=auth["tokenUrl"],
        headers=auth.get("headers", {}),
        data=data,
        timeout=60,
    )
    token_resp.raise_for_status()
    token = token_resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}, None


def _auth_oauth2_form_client_credentials(auth: dict):
    data = {"grant_type": "client_credentials"}
    basic_auth = HTTPBasicAuth(auth.get("clientId"), auth.get("clientSecret"))
    token_resp = requests.post(
        url=auth["tokenUrl"],
        headers=auth.get("headers", {}),
        auth=basic_auth,
        data=data,
        timeout=60,
    )
    token_resp.raise_for_status()
    token = token_resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}, None


def _auth_oauth2_client_credentials_json(auth: dict):
    client_id_key = auth.get("client_id_key")
    client_secret_key = auth.get("client_secret_key")
    grant_type_key = auth.get("grant_type_key")
    client_id_value = auth.get("client_id_value")
    client_secret_value = auth.get("client_secret_value")
    grant_type_value = auth.get("grant_type_value")
    scope_key = auth.get("scope_key")
    scope_value = auth.get("scope_value")

    json_data = {
        grant_type_key: grant_type_value,
        client_id_key: client_id_value,
        client_secret_key: client_secret_value,
    }
    if scope_key and scope_value:
        json_data[scope_key] = scope_value
    resp = requests.post(
        auth["tokenUrl"],
        headers=auth.get("headers", {}),
        json=json_data,
        timeout=60,
    )
    resp.raise_for_status()
    return {"Authorization": f"Bearer {resp.json()['access_token']}"}, None


def _auth_oauth2_client_credentials_form(auth: dict):
    client_id_key = auth.get("client_id_key")
    client_secret_key = auth.get("client_secret_key")
    client_id_value = auth.get("client_id_value")
    client_secret_value = auth.get("client_secret_value")
    grant_type_key = auth.get("grant_type_key")
    grant_type_value = auth.get("grant_type_value")
    scope_key = auth.get("scope_key")
    scope_value = auth.get("scope_value")
    data = {
        client_id_key: client_id_value,
        client_secret_key: client_secret_value,
        grant_type_key: grant_type_value,
    }
    if scope_key and scope_value:
        data[scope_key] = scope_value
    resp = requests.post(
        auth["tokenUrl"],
        headers=auth.get("headers", {}),
        data=data,
        timeout=60,
    )
    resp.raise_for_status()
    return {"Authorization": f"Bearer {resp.json()['access_token']}"}, None


def _auth_oauth2_client_credentials_basic(auth: dict):
    client_id_value = auth.get("client_id_value")
    client_secret_value = auth.get("client_secret_value")
    grant_type_key = auth.get("grant_type_key")
    grant_type_value = auth.get("grant_type_value")
    scope_key = auth.get("scope_key")
    scope_value = auth.get("scope_value")
    data = {grant_type_key: grant_type_value}
    if scope_key and scope_value:
        data[scope_key] = scope_value
    resp = requests.post(
        auth["tokenUrl"],
        headers=auth.get("headers", {}),
        auth=HTTPBasicAuth(client_id_value, client_secret_value),
        data=data,
        timeout=60,
    )
    resp.raise_for_status()
    return {"Authorization": f"Bearer {resp.json()['access_token']}"}, None


def _auth_oauth2_password_form(auth: dict):
    username_key = auth.get("username_key")
    password_key = auth.get("password_key")
    username_value = auth.get("username_value")
    password_value = auth.get("password_value")
    grant_type_key = auth.get("grant_type_key")
    grant_type_value = auth.get("grant_type_value")

    data = {
        username_key: username_value,
        password_key: password_value,
        grant_type_key: grant_type_value,
    }

    response = requests.post(
        auth["tokenUrl"],
        headers=auth.get("headers", {}),
        data=data,
        timeout=60,
    )
    response.raise_for_status()
    token = response.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}, None


def _auth_oauth2_password_json(auth: dict):
    username_key = auth.get("username_key")
    password_key = auth.get("password_key")
    username_value = auth.get("username_value")
    password_value = auth.get("password_value")

    data = {
        username_key: username_value,
        password_key: password_value,
    }

    response = requests.post(
        auth["tokenUrl"],
        headers=auth.get("headers", {}),
        json=data,
        timeout=60,
    )
    response.raise_for_status()
    token = response.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}, None


# Dispatch table mapping `authentication.type` to its handler. Keeping each
# flow in its own function (rather than one long if/elif chain) keeps
# `get_auth_headers` simple and satisfies the cyclomatic-complexity lint.
_AUTH_HANDLERS = {
    "basic": _auth_basic,
    "bearer": _auth_bearer,
    "apikey": _auth_apikey,
    "oauth2_assertion": _auth_oauth2_assertion,
    "oauth2_client_credentials": _auth_oauth2_client_credentials,
    "oauth2_form_client_credentials": _auth_oauth2_form_client_credentials,
    "oauth2_client_credentials_json": _auth_oauth2_client_credentials_json,
    "oauth2_client_credentials_form": _auth_oauth2_client_credentials_form,
    "oauth2_client_credentials_basic": _auth_oauth2_client_credentials_basic,
    "oauth2_password_form": _auth_oauth2_password_form,
    "oauth2_password_json": _auth_oauth2_password_json,
}


def get_auth_headers(auth):
    """Builds the (headers, requests-auth) pair for an `authentication`
    config block. Supports basic, bearer, apikey (header/query), mTLS
    (handled by the caller via cert files) and all OAuth2 client-credentials
    / password / assertion flows used across the example configs."""
    if not auth or auth.get("type") == "none":
        return {}, None

    handler = _AUTH_HANDLERS.get(auth["type"])
    if handler is None:
        return {}, None
    return handler(auth)
