"""Unit tests for `rest_ds.authentication.auth_util.get_auth_headers`.

This module is the single canonical implementation of every auth flow used
by both `rest_ds.rest_api.APIClient` and `rest_ds.util.request_builder`
(the incremental runner's dependency), so it is the highest-value place to
have solid, fast, network-free unit test coverage.
"""

import datetime

import pytest
from requests.auth import HTTPBasicAuth

from rest_ds.authentication.auth_util import get_auth_headers


def test_none_auth_returns_empty():
    assert get_auth_headers(None) == ({}, None)
    assert get_auth_headers({"type": "none"}) == ({}, None)


def test_basic_auth():
    headers, auth = get_auth_headers(
        {"type": "basic", "username": "alice", "password": "s3cret"}
    )
    assert headers == {}
    assert isinstance(auth, HTTPBasicAuth)
    assert auth.username == "alice"
    assert auth.password == "s3cret"


def test_bearer_auth():
    headers, auth = get_auth_headers({"type": "bearer", "token": "abc123"})
    assert headers == {"Authorization": "Bearer abc123"}
    assert auth is None


def test_apikey_header_uses_name_value_convention():
    headers, auth = get_auth_headers(
        {"type": "apikey", "in": "header", "name": "X-Api-Key", "value": "key1"}
    )
    assert headers == {"X-Api-Key": "key1"}
    assert auth is None


def test_apikey_header_uses_api_key_name_value_convention():
    headers, auth = get_auth_headers(
        {
            "type": "apikey",
            "in": "header",
            "api_key_name": "X-Api-Key",
            "api_key_value": "key2",
        }
    )
    assert headers == {"X-Api-Key": "key2"}
    assert auth is None


def test_apikey_query_returns_empty_headers():
    # Query-param injection is the caller's responsibility (see
    # `build_request_components` / `APIClient._build_components`).
    headers, auth = get_auth_headers(
        {"type": "apikey", "in": "query", "name": "apikey", "value": "key3"}
    )
    assert headers == {}
    assert auth is None


def test_oauth2_client_credentials(requests_mock):
    requests_mock.post(
        "https://auth.example.com/token", json={"access_token": "tok-cc"}
    )
    headers, auth = get_auth_headers(
        {
            "type": "oauth2_client_credentials",
            "tokenUrl": "https://auth.example.com/token",
            "clientId": "id1",
            "clientSecret": "secret1",
            "scope": "read",
        }
    )
    assert headers == {"Authorization": "Bearer tok-cc"}
    assert auth is None
    sent = requests_mock.last_request
    assert sent.text == (
        "grant_type=client_credentials&client_id=id1&client_secret=secret1&scope=read"
    )


def test_oauth2_form_client_credentials_uses_basic_auth(requests_mock):
    requests_mock.post(
        "https://auth.example.com/token", json={"access_token": "tok-form"}
    )
    headers, auth = get_auth_headers(
        {
            "type": "oauth2_form_client_credentials",
            "tokenUrl": "https://auth.example.com/token",
            "clientId": "id2",
            "clientSecret": "secret2",
        }
    )
    assert headers == {"Authorization": "Bearer tok-form"}
    assert auth is None
    sent = requests_mock.last_request
    assert sent.text == "grant_type=client_credentials"
    assert sent.headers["Authorization"].startswith("Basic ")


@pytest.mark.parametrize(
    "auth_type,expects_basic_auth",
    [
        ("oauth2_client_credentials_json", False),
        ("oauth2_client_credentials_form", False),
        ("oauth2_client_credentials_basic", True),
    ],
)
def test_oauth2_client_credentials_generic_variants(
    requests_mock, auth_type, expects_basic_auth
):
    requests_mock.post("https://auth.example.com/token", json={"access_token": "tok-x"})
    auth_cfg = {
        "type": auth_type,
        "tokenUrl": "https://auth.example.com/token",
        "grant_type_key": "grant_type",
        "grant_type_value": "client_credentials",
        "client_id_key": "client_id",
        "client_id_value": "id3",
        "client_secret_key": "client_secret",
        "client_secret_value": "secret3",
        "scope_key": "scope",
        "scope_value": "read write",
    }
    headers, auth = get_auth_headers(auth_cfg)
    assert headers == {"Authorization": "Bearer tok-x"}
    # The returned `auth` is always None for OAuth2 flows: any Basic-Auth
    # credentials are only used for the token-exchange request itself, not
    # for subsequent API calls (which use the returned bearer header).
    assert auth is None
    sent = requests_mock.last_request
    if expects_basic_auth:
        assert sent.headers["Authorization"].startswith("Basic ")
    else:
        assert "Authorization" not in sent.headers


def test_oauth2_password_form(requests_mock):
    requests_mock.post(
        "https://auth.example.com/token", json={"access_token": "tok-pw"}
    )
    headers, auth = get_auth_headers(
        {
            "type": "oauth2_password_form",
            "tokenUrl": "https://auth.example.com/token",
            "username_key": "username",
            "username_value": "bob",
            "password_key": "password",
            "password_value": "hunter2",
            "grant_type_key": "grant_type",
            "grant_type_value": "password",
        }
    )
    assert headers == {"Authorization": "Bearer tok-pw"}
    assert auth is None


def test_oauth2_password_json(requests_mock):
    requests_mock.post(
        "https://auth.example.com/token", json={"access_token": "tok-pw-json"}
    )
    headers, auth = get_auth_headers(
        {
            "type": "oauth2_password_json",
            "tokenUrl": "https://auth.example.com/token",
            "username_key": "username",
            "username_value": "bob",
            "password_key": "password",
            "password_value": "hunter2",
        }
    )
    assert headers == {"Authorization": "Bearer tok-pw-json"}
    assert auth is None


def test_oauth2_password_form_raises_on_http_error(requests_mock):
    requests_mock.post(
        "https://auth.example.com/token", status_code=401, json={"error": "denied"}
    )
    with pytest.raises(Exception):
        get_auth_headers(
            {
                "type": "oauth2_password_form",
                "tokenUrl": "https://auth.example.com/token",
                "username_key": "username",
                "username_value": "bob",
                "password_key": "password",
                "password_value": "wrong",
            }
        )


def test_unknown_auth_type_returns_empty():
    assert get_auth_headers({"type": "totally-unsupported"}) == ({}, None)


def test_oauth2_assertion(requests_mock, tmp_path):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.utcnow())
        .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=1))
        .sign(key, hashes.SHA256())
    )

    public_key_path = tmp_path / "public.pem"
    private_key_path = tmp_path / "private.pem"
    public_key_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    private_key_path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )

    requests_mock.post(
        "https://auth.example.com/token", json={"access_token": "tok-assert"}
    )

    headers, auth = get_auth_headers(
        {
            "type": "oauth2_assertion",
            "tokenUrl": "https://auth.example.com/token",
            "public_key_path": str(public_key_path),
            "private_key_path": str(private_key_path),
            "aud": "https://api.example.com",
            "grant_type_key": "grant_type",
            "grant_type_value": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        }
    )
    assert headers == {"Authorization": "Bearer tok-assert"}
    assert auth is None
    sent_body = requests_mock.last_request.json()
    assert sent_body["grant_type"] == "urn:ietf:params:oauth:grant-type:jwt-bearer"
    assert "assertion" in sent_body
