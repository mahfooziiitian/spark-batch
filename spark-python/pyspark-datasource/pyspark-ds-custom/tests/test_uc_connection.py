"""Tests for Unity Catalog HTTP connection support in REST API data sources."""

from __future__ import annotations

import threading
import time
from urllib.parse import parse_qs

import pytest
import uvicorn
from faker import Faker
from fastapi import FastAPI, HTTPException, Request

from custom_ds import (
    RestApiArrowDataSource,
    RestApiDataSource,
    RestApiSinkDataSource,
    create_spark_session,
)
from custom_ds.restapi.uc_connection import UCConnectionConfig

pytestmark = pytest.mark.pyspark

# ---------------------------------------------------------------------------
# Mock server simulating a UC-connected API
# ---------------------------------------------------------------------------
_UC_PORT = 19096
_uc_app = FastAPI()
_fake = Faker()
Faker.seed(42)

UC_BEARER_TOKEN = "uc-injected-bearer-token-xyz"
M2M_CLIENT_ID = "m2m-test-client"
M2M_CLIENT_SECRET = "m2m-test-secret"
M2M_ACCESS_TOKEN = "m2m-access-token-abc123"

_USERS = [
    {"id": i, "name": _fake.name(), "email": _fake.email(), "city": _fake.city()}
    for i in range(1, 4)
]

_WRITTEN: list[dict] = []


def _check_bearer(request: Request, expected_token: str) -> None:
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {expected_token}":
        raise HTTPException(status_code=401, detail="Unauthorized")


@_uc_app.get("/v2/users")
def get_users(request: Request):
    auth = request.headers.get("Authorization", "")
    # Accept both bearer token and M2M token
    if auth == f"Bearer {UC_BEARER_TOKEN}" or auth == f"Bearer {M2M_ACCESS_TOKEN}":
        return _USERS
    raise HTTPException(status_code=401, detail="Unauthorized")


@_uc_app.post("/v2/records")
async def post_records(request: Request):
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {UC_BEARER_TOKEN}" and auth != f"Bearer {M2M_ACCESS_TOKEN}":
        raise HTTPException(status_code=401, detail="Unauthorized")
    body = await request.json()
    if isinstance(body, list):
        _WRITTEN.extend(body)
    else:
        _WRITTEN.append(body)
    return {"status": "ok", "count": len(body) if isinstance(body, list) else 1}


@_uc_app.post("/oauth/m2m/token")
async def m2m_token(request: Request):
    """Mock OAuth M2M token endpoint."""
    body = await request.body()
    params = parse_qs(body.decode())
    cid = (params.get("client_id") or [""])[0]
    csecret = (params.get("client_secret") or [""])[0]

    if cid != M2M_CLIENT_ID or csecret != M2M_CLIENT_SECRET:
        raise HTTPException(status_code=401, detail="invalid_client")

    return {
        "access_token": M2M_ACCESS_TOKEN,
        "token_type": "bearer",
        "expires_in": 3600,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def _uc_server():
    """Start the mock UC API server in a daemon thread."""
    config = uvicorn.Config(_uc_app, host="127.0.0.1", port=_UC_PORT, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    time.sleep(1.5)
    yield
    server.should_exit = True


@pytest.fixture(scope="module")
def spark():
    session = create_spark_session("test-uc-connection")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Unit tests -- UCConnectionConfig
# ---------------------------------------------------------------------------
class TestUCConnectionConfig:
    def test_from_options_returns_none_when_not_configured(self):
        config = UCConnectionConfig.from_options({"url": "http://example.com"})
        assert config is None

    def test_from_options_with_uc_prefix(self):
        config = UCConnectionConfig.from_options(
            {
                "uc.host": "https://api.example.com",
                "uc.basePath": "/v2",
                "uc.bearerToken": "my-token",
            }
        )
        assert config is not None
        assert config.host == "https://api.example.com"
        assert config.base_path == "/v2"
        assert config.bearer_token == "my-token"
        assert config.auth_type == "bearer_token"

    def test_from_options_with_injected_keys(self):
        config = UCConnectionConfig.from_options(
            {
                "databricks.connection": "my_conn",
                "host": "https://injected.example.com",
                "base_path": "/api",
                "bearer_token": "injected-token",
            }
        )
        assert config is not None
        assert config.host == "https://injected.example.com"
        assert config.bearer_token == "injected-token"
        assert config.connection_name == "my_conn"
        assert config.auth_type == "bearer_token"

    def test_injected_keys_take_precedence(self):
        config = UCConnectionConfig.from_options(
            {
                "databricks.connection": "my_conn",
                "host": "https://injected.com",
                "uc.host": "https://local.com",
                "bearer_token": "injected-token",
                "uc.bearerToken": "local-token",
            }
        )
        assert config.host == "https://injected.com"
        assert config.bearer_token == "injected-token"

    def test_oauth_m2m_from_options(self):
        config = UCConnectionConfig.from_options(
            {
                "uc.host": "https://api.example.com",
                "uc.clientId": "my-client",
                "uc.clientSecret": "my-secret",
                "uc.tokenEndpoint": "https://auth.example.com/token",
                "uc.oauthScope": "read write",
            }
        )
        assert config is not None
        assert config.auth_type == "oauth_m2m"
        assert config.client_id == "my-client"
        assert config.client_secret == "my-secret"
        assert config.token_endpoint == "https://auth.example.com/token"
        assert config.oauth_scope == "read write"

    def test_oauth_m2m_injected_keys(self):
        config = UCConnectionConfig.from_options(
            {
                "databricks.connection": "my_oauth_conn",
                "host": "https://api.example.com",
                "client_id": "injected-client",
                "client_secret": "injected-secret",
                "token_endpoint": "https://auth.example.com/token",
            }
        )
        assert config.auth_type == "oauth_m2m"
        assert config.client_id == "injected-client"
        assert config.connection_name == "my_oauth_conn"

    def test_bearer_takes_precedence_over_oauth_m2m(self):
        config = UCConnectionConfig.from_options(
            {
                "uc.host": "https://api.example.com",
                "uc.bearerToken": "my-token",
                "uc.clientId": "my-client",
                "uc.clientSecret": "my-secret",
            }
        )
        assert config.auth_type == "bearer_token"

    def test_build_base_url(self):
        config = UCConnectionConfig(host="https://api.example.com", base_path="/v2")
        assert config.build_base_url() == "https://api.example.com/v2"

    def test_build_base_url_with_port(self):
        config = UCConnectionConfig(host="https://api.example.com", port="8443", base_path="/v2")
        url = config.build_base_url()
        assert ":8443" in url
        assert url.endswith("/v2")

    def test_resolve_url_with_user_url(self):
        config = UCConnectionConfig(host="https://api.example.com")
        assert (
            config.resolve_url("https://override.com/endpoint") == "https://override.com/endpoint"
        )

    def test_resolve_url_with_path(self):
        config = UCConnectionConfig(host="https://api.example.com", base_path="/v2")
        assert config.resolve_url(None, "users") == "https://api.example.com/v2/users"

    def test_apply_auth_headers_bearer(self):
        config = UCConnectionConfig(
            host="https://api.example.com",
            bearer_token="tok123",
            auth_type="bearer_token",
        )
        headers = config.apply_auth_headers({"Accept": "application/json"})
        assert headers["Authorization"] == "Bearer tok123"
        assert headers["Accept"] == "application/json"

    def test_apply_auth_headers_no_auth(self):
        config = UCConnectionConfig(host="https://api.example.com")
        headers = {"Accept": "application/json"}
        result = config.apply_auth_headers(headers)
        assert "Authorization" not in result

    def test_fetch_oauth_token_missing_endpoint(self):
        config = UCConnectionConfig(
            host="https://api.example.com",
            client_id="client",
            client_secret="secret",
            auth_type="oauth_m2m",
        )
        with pytest.raises(ValueError, match="token_endpoint is required"):
            config.fetch_oauth_token()

    def test_fetch_oauth_token_missing_credentials(self):
        config = UCConnectionConfig(
            host="https://api.example.com",
            token_endpoint="https://auth.example.com/token",
            auth_type="oauth_m2m",
        )
        with pytest.raises(ValueError, match="client_id and client_secret"):
            config.fetch_oauth_token()


class TestUCOAuthM2MToken:
    def test_fetch_oauth_token(self, _uc_server):
        config = UCConnectionConfig(
            host=f"http://127.0.0.1:{_UC_PORT}",
            client_id=M2M_CLIENT_ID,
            client_secret=M2M_CLIENT_SECRET,
            token_endpoint=f"http://127.0.0.1:{_UC_PORT}/oauth/m2m/token",
            auth_type="oauth_m2m",
        )
        token = config.fetch_oauth_token()
        assert token == M2M_ACCESS_TOKEN

    def test_fetch_oauth_token_bad_credentials(self, _uc_server):
        config = UCConnectionConfig(
            host=f"http://127.0.0.1:{_UC_PORT}",
            client_id="wrong-client",
            client_secret="wrong-secret",
            token_endpoint=f"http://127.0.0.1:{_UC_PORT}/oauth/m2m/token",
            auth_type="oauth_m2m",
        )
        with pytest.raises(RuntimeError, match="OAuth M2M token request failed"):
            config.fetch_oauth_token()

    def test_apply_auth_headers_m2m(self, _uc_server):
        config = UCConnectionConfig(
            host=f"http://127.0.0.1:{_UC_PORT}",
            client_id=M2M_CLIENT_ID,
            client_secret=M2M_CLIENT_SECRET,
            token_endpoint=f"http://127.0.0.1:{_UC_PORT}/oauth/m2m/token",
            auth_type="oauth_m2m",
        )
        headers = config.apply_auth_headers({})
        assert headers["Authorization"] == f"Bearer {M2M_ACCESS_TOKEN}"


# ---------------------------------------------------------------------------
# Integration tests -- UC connection with Spark data sources
# ---------------------------------------------------------------------------
class TestUCConnectionBatchRead:
    def test_batch_read_with_bearer_token(self, spark, _uc_server):
        spark.dataSource.register(RestApiDataSource)
        df = (
            spark.read.format("restapi")
            .option("uc.host", f"http://127.0.0.1:{_UC_PORT}")
            .option("uc.basePath", "/v2")
            .option("uc.bearerToken", UC_BEARER_TOKEN)
            .option("uc.path", "users")
            .schema("id INT, name STRING, email STRING, city STRING")
            .load()
        )
        assert df.count() == 3
        row = df.filter("id = 1").first()
        assert row is not None
        assert row["name"] is not None

    def test_batch_read_with_oauth_m2m(self, spark, _uc_server):
        spark.dataSource.register(RestApiDataSource)
        df = (
            spark.read.format("restapi")
            .option("uc.host", f"http://127.0.0.1:{_UC_PORT}")
            .option("uc.basePath", "/v2")
            .option("uc.clientId", M2M_CLIENT_ID)
            .option("uc.clientSecret", M2M_CLIENT_SECRET)
            .option("uc.tokenEndpoint", f"http://127.0.0.1:{_UC_PORT}/oauth/m2m/token")
            .option("uc.path", "users")
            .schema("id INT, name STRING, email STRING, city STRING")
            .load()
        )
        assert df.count() == 3


class TestUCConnectionArrowRead:
    def test_arrow_read_with_uc_options(self, spark, _uc_server):
        spark.dataSource.register(RestApiArrowDataSource)
        df = (
            spark.read.format("restapi_arrow")
            .option("uc.host", f"http://127.0.0.1:{_UC_PORT}")
            .option("uc.basePath", "/v2")
            .option("uc.bearerToken", UC_BEARER_TOKEN)
            .option("uc.path", "users")
            .schema("id STRING, name STRING, email STRING, city STRING")
            .load()
        )
        assert df.count() == 3


class TestUCConnectionBatchWrite:
    def test_batch_write_with_uc_options(self, spark, _uc_server):
        _WRITTEN.clear()
        spark.dataSource.register(RestApiSinkDataSource)
        data = [(100, "Test User", "test@test.com", "TestCity")]
        df = spark.createDataFrame(data, ["id", "name", "email", "city"])
        df.write.format("restapi_sink").option("uc.host", f"http://127.0.0.1:{_UC_PORT}").option(
            "uc.basePath", "/v2"
        ).option("uc.bearerToken", UC_BEARER_TOKEN).option("uc.path", "records").mode(
            "append"
        ).save()
        assert len(_WRITTEN) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
