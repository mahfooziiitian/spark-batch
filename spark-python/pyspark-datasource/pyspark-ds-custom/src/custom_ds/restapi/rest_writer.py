"""REST API batch sink — write DataFrame rows to an HTTP endpoint via POST.

Demonstrates implementing DataSourceWriter for REST APIs. Each partition sends
rows as a JSON array to the configured endpoint.

Usage:
    spark.dataSource.register(RestApiSinkDataSource)

    df.write.format("restapi_sink") \\
        .option("url", "http://localhost:8000/api/records") \\
        .option("batchSize", "50") \\
        .mode("append") \\
        .save()
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field

from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType


@dataclass
class RestApiWriteCommitMessage(WriterCommitMessage):
    """Tracks how many rows and requests were sent by each task."""

    rows_sent: int
    requests_made: int
    status_codes: list = field(default_factory=list)


class RestApiSinkDataSource(DataSource):
    """A batch sink that POSTs DataFrame rows as JSON to a REST API endpoint.

    Options:
        url (str): Required. The target HTTP endpoint.
        batchSize (int): Number of rows per HTTP request. Default 100 (all rows in one batch).
        headers.<name> (str): Custom headers to include in each request.
        apiKey (str): API key value (sent via header).
        apiKeyHeader (str): Header name for the API key. Default "X-API-Key".
        auth (str): Set to "oauth2" for OAuth2 authentication.
        oauth.tokenUrl (str): OAuth2 token endpoint URL.
        oauth.clientId (str): OAuth2 client ID.
        oauth.clientSecret (str): OAuth2 client secret.
        oauth.grantType (str): OAuth2 grant type. Default "client_credentials".
        oauth.scope (str): OAuth2 scope.
        oauth.bearerToken (str): Pre-obtained bearer token.
    """

    @classmethod
    def name(cls) -> str:
        return "restapi_sink"

    def schema(self) -> str:
        return "id LONG, value STRING"

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        return RestApiSinkWriter(schema, self.options, overwrite)


class RestApiSinkWriter(DataSourceWriter):
    def __init__(self, schema: StructType, options: Mapping[str, str], overwrite: bool) -> None:
        self.url = options.get("url")

        # Resolve UC HTTP connection
        from custom_ds.restapi.uc_connection import UCConnectionConfig

        uc_config = UCConnectionConfig.from_options(options)
        if not self.url and uc_config is not None:
            uc_path = options.get("uc.path") or options.get("uc.Path") or ""
            self.url = uc_config.resolve_url(None, uc_path)

        if not self.url:
            raise ValueError("Option 'url', 'databricks.connection', or 'uc.host' is required")

        self.batch_size = int(options.get("batchSize") or options.get("batchsize") or 100)
        self.headers = {"Content-Type": "application/json"}
        self.headers.update(_extract_prefixed_options(options, "headers."))
        _apply_api_key(options, self.headers)

        # Apply UC bearer token
        if uc_config is not None:
            self.headers = uc_config.apply_auth_headers(self.headers)

        from custom_ds.restapi.oauth import OAuth2Config

        self.oauth_config = OAuth2Config.from_options(options)

    def write(self, iterator: Iterator[Row]) -> RestApiWriteCommitMessage:
        batch: list[dict] = []
        rows_sent = 0
        requests_made = 0
        status_codes: list[int] = []

        for row in iterator:
            batch.append(row.asDict())
            if len(batch) >= self.batch_size:
                status = self._send_batch(batch)
                status_codes.append(status)
                rows_sent += len(batch)
                requests_made += 1
                batch = []

        if batch:
            status = self._send_batch(batch)
            status_codes.append(status)
            rows_sent += len(batch)
            requests_made += 1

        return RestApiWriteCommitMessage(
            rows_sent=rows_sent, requests_made=requests_made, status_codes=status_codes
        )

    def _send_batch(self, batch: list[dict]) -> int:
        import requests as req

        headers = self.headers
        if self.oauth_config is not None:
            headers = self.oauth_config.apply_to_headers(headers)
        assert self.url is not None
        response = req.post(self.url, json=batch, headers=headers, timeout=30)
        response.raise_for_status()
        return response.status_code

    def commit(self, messages: list) -> None:
        total_rows = sum(m.rows_sent for m in messages if m is not None)
        total_requests = sum(m.requests_made for m in messages if m is not None)
        print(f"[restapi_sink] committed {total_rows} rows via {total_requests} HTTP requests")

    def abort(self, messages: list) -> None:
        print("[restapi_sink] write aborted — partial data may have been sent to the endpoint")


# ---------------------------------------------------------------------------
# Helpers (duplicated from rest_source to keep each module self-contained)
# ---------------------------------------------------------------------------


def _extract_prefixed_options(options: Mapping[str, str], prefix: str) -> dict[str, str]:
    result = {}
    for key, value in options.items():
        if key.lower().startswith(prefix.lower()):
            result[key[len(prefix) :]] = value
    return result


def _apply_api_key(options: Mapping[str, str], headers: dict[str, str]) -> None:
    api_key = options.get("apiKey") or options.get("apikey")
    if api_key:
        header_name = options.get("apiKeyHeader") or options.get("apikeyheader") or "X-API-Key"
        headers[header_name] = api_key
