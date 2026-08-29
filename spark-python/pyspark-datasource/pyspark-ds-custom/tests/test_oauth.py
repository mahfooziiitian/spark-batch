"""Tests for OAuth2 authentication in REST API data sources."""

from __future__ import annotations

import threading
import time

import pytest
import uvicorn
from faker import Faker
from fastapi import FastAPI, HTTPException, Request

from custom_ds import RestApiDataSource, RestApiSinkDataSource, create_spark_session
from custom_ds.restapi.oauth import OAuth2Config

pytestmark = pytest.mark.pyspark

# ---------------------------------------------------------------------------
# Mock OAuth2 server
# ---------------------------------------------------------------------------
_OAUTH_PORT = 19095
_oauth_app = FastAPI()
_fake = Faker()
Faker.seed(99)

VALID_CLIENT_ID = "test-client"
VALID_CLIENT_SECRET = "test-secret"
MOCK_ACCESS_TOKEN = "mock-access-token-12345"

_USERS = [
    {
        "id": i,
        "name": _fake.name(),
        "email": _fake.email(),
        "city": _fake.city(),
        "age": _fake.random_int(20, 60),
    }
    for i in range(1, 6)
]

_WRITTEN: list[dict] = []


@_oauth_app.post("/oauth/token")
async def oauth_token(request: Request):
    body = await request.body()
    from urllib.parse import parse_qs

    params = parse_qs(body.decode())
    client_id = (params.get("client_id") or [""])[0]
    client_secret = (params.get("client_secret") or [""])[0]

    if client_id != VALID_CLIENT_ID or client_secret != VALID_CLIENT_SECRET:
        raise HTTPException(status_code=401, detail="invalid_client")

    return {
        "access_token": MOCK_ACCESS_TOKEN,
        "token_type": "bearer",
        "expires_in": 3600,
    }


@_oauth_app.get("/api/protected/users")
def get_protected_users(request: Request):
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {MOCK_ACCESS_TOKEN}":
        raise HTTPException(status_code=401, detail="Unauthorized")
    return {"data": _USERS, "total": len(_USERS)}


@_oauth_app.post("/api/records")
async def write_records(request: Request):
    body = await request.json()
    if isinstance(body, list):
        _WRITTEN.extend(body)
    else:
        _WRITTEN.append(body)
    return {"status": "ok", "received": len(body) if isinstance(body, list) else 1}


@pytest.fixture(scope="module")
def oauth_server():
    """Start mock server with OAuth endpoints on port 19095."""
    config = uvicorn.Config(_oauth_app, host="127.0.0.1", port=_OAUTH_PORT, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    time.sleep(1)
    yield f"http://127.0.0.1:{_OAUTH_PORT}"
    server.should_exit = True


@pytest.fixture(scope="module")
def spark():
    session = create_spark_session("test-oauth")
    session.dataSource.register(RestApiDataSource)
    session.dataSource.register(RestApiSinkDataSource)
    yield session
    session.stop()


class TestOAuth2Config:
    """Unit tests for OAuth2Config parsing and token fetch."""

    def test_from_options_returns_none_without_auth(self):
        config = OAuth2Config.from_options({"url": "http://example.com"})
        assert config is None

    def test_from_options_parses_client_credentials(self):
        options = {
            "auth": "oauth2",
            "oauth.tokenUrl": "http://auth/token",
            "oauth.clientId": "my-client",
            "oauth.clientSecret": "my-secret",
            "oauth.scope": "read write",
        }
        config = OAuth2Config.from_options(options)
        assert config is not None
        assert config.token_url == "http://auth/token"
        assert config.client_id == "my-client"
        assert config.client_secret == "my-secret"
        assert config.scope == "read write"
        assert config.grant_type == "client_credentials"

    def test_from_options_parses_bearer_token(self):
        options = {
            "auth": "oauth2",
            "oauth.bearerToken": "pre-obtained-token",
        }
        config = OAuth2Config.from_options(options)
        assert config is not None
        assert config.fetch_token() == "pre-obtained-token"

    def test_from_options_parses_password_grant(self):
        options = {
            "auth": "oauth2",
            "oauth.tokenUrl": "http://auth/token",
            "oauth.clientId": "c",
            "oauth.clientSecret": "s",
            "oauth.grantType": "password",
            "oauth.username": "user",
            "oauth.password": "pass",
        }
        config = OAuth2Config.from_options(options)
        assert config is not None
        assert config.grant_type == "password"
        assert config.username == "user"

    def test_fetch_token_from_mock_server(self, oauth_server):
        config = OAuth2Config(
            token_url=f"{oauth_server}/oauth/token",
            client_id="test-client",
            client_secret="test-secret",
            grant_type="client_credentials",
        )
        token = config.fetch_token()
        assert token == "mock-access-token-12345"

    def test_fetch_token_invalid_credentials(self, oauth_server):
        config = OAuth2Config(
            token_url=f"{oauth_server}/oauth/token",
            client_id="wrong",
            client_secret="wrong",
        )
        with pytest.raises(Exception):  # noqa: B017
            config.fetch_token()

    def test_apply_to_headers(self, oauth_server):
        config = OAuth2Config(
            token_url=f"{oauth_server}/oauth/token",
            client_id="test-client",
            client_secret="test-secret",
        )
        headers = config.apply_to_headers({"Accept": "application/json"})
        assert headers["Authorization"] == "Bearer mock-access-token-12345"
        assert headers["Accept"] == "application/json"


class TestOAuth2Integration:
    """Integration tests — OAuth2 with Spark data source reads/writes."""

    def test_batch_read_with_oauth2(self, spark, oauth_server):
        """Read from a protected endpoint using OAuth2 client_credentials."""
        df = (
            spark.read.format("restapi")
            .option("url", f"{oauth_server}/api/protected/users")
            .option("resultKey", "data")
            .option("schema", "id LONG, name STRING, email STRING, city STRING, age LONG")
            .option("auth", "oauth2")
            .option("oauth.tokenUrl", f"{oauth_server}/oauth/token")
            .option("oauth.clientId", "test-client")
            .option("oauth.clientSecret", "test-secret")
            .load()
        )

        assert df.count() == 5
        row = df.collect()[0]
        assert "name" in row.asDict()

    def test_batch_read_fails_without_auth(self, spark, oauth_server):
        """Protected endpoint returns 401 without OAuth2."""
        df = (
            spark.read.format("restapi")
            .option("url", f"{oauth_server}/api/protected/users")
            .option("resultKey", "data")
            .option("schema", "id LONG, name STRING")
            .load()
        )
        with pytest.raises(Exception):  # noqa: B017
            df.collect()

    def test_batch_read_with_bearer_token(self, spark, oauth_server):
        """Use a pre-obtained bearer token (skip token endpoint)."""
        df = (
            spark.read.format("restapi")
            .option("url", f"{oauth_server}/api/protected/users")
            .option("resultKey", "data")
            .option("schema", "id LONG, name STRING, email STRING, city STRING, age LONG")
            .option("auth", "oauth2")
            .option("oauth.bearerToken", "mock-access-token-12345")
            .load()
        )

        assert df.count() == 5

    def test_batch_write_with_oauth2(self, spark, oauth_server):
        """Write to an endpoint using OAuth2 (standard endpoint accepts any auth)."""
        from pyspark.sql import functions as F

        df = spark.range(3).select(
            F.col("id"),
            F.concat(F.lit("oauth-"), F.col("id").cast("string")).alias("value"),
        )

        # Write to standard endpoint (no auth required, but OAuth headers sent)
        df.write.format("restapi_sink").option("url", f"{oauth_server}/api/records").option(
            "auth", "oauth2"
        ).option("oauth.tokenUrl", f"{oauth_server}/oauth/token").option(
            "oauth.clientId", "test-client"
        ).option("oauth.clientSecret", "test-secret").mode("append").save()
