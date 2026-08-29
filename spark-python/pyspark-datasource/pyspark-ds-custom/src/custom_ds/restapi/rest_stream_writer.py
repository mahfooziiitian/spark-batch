"""REST API streaming sink — write micro-batch data to an HTTP endpoint.

Demonstrates implementing DataSourceStreamWriter for REST APIs. Each micro-batch
POSTs its rows as JSON to the configured endpoint, with commit/abort tracking.

Usage:
    spark.dataSource.register(RestApiStreamSinkDataSource)

    query = df.writeStream.format("restapi_stream_sink") \\
        .option("url", "http://localhost:9090/api/records") \\
        .option("batchSize", "50") \\
        .start()
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass

from pyspark.sql import Row
from pyspark.sql.datasource import (
    DataSource,
    DataSourceStreamWriter,
    WriterCommitMessage,
)
from pyspark.sql.types import StructType


@dataclass
class StreamWriteCommitMessage(WriterCommitMessage):
    """Tracks rows sent and HTTP status for each partition in a micro-batch."""

    partition_id: int
    rows_sent: int
    success: bool


class RestApiStreamSinkDataSource(DataSource):
    """A streaming sink that POSTs each micro-batch to a REST API endpoint.

    Options:
        url (str): Required. The target HTTP endpoint.
        batchSize (int): Number of rows per HTTP request. Default 100.
        headers.<name> (str): Custom headers.
        apiKey (str): API key value.
        apiKeyHeader (str): Header name for API key. Default "X-API-Key".
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
        return "restapi_stream_sink"

    def schema(self) -> str:
        return "id LONG, value STRING"

    def streamWriter(self, schema: StructType, overwrite: bool) -> DataSourceStreamWriter:
        return RestApiStreamWriter(schema, self.options)


class RestApiStreamWriter(DataSourceStreamWriter):
    def __init__(self, schema: StructType, options: Mapping[str, str]) -> None:
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
        self.headers: dict = {"Content-Type": "application/json"}
        for key, value in options.items():
            if key.lower().startswith("headers."):
                self.headers[key[8:]] = value
        api_key = options.get("apiKey") or options.get("apikey")
        if api_key:
            header_name = options.get("apiKeyHeader") or options.get("apikeyheader") or "X-API-Key"
            self.headers[header_name] = api_key

        # Apply UC bearer token
        if uc_config is not None:
            self.headers = uc_config.apply_auth_headers(self.headers)

        from custom_ds.restapi.oauth import OAuth2Config

        self.oauth_config = OAuth2Config.from_options(options)

    def write(self, iterator: Iterator[Row]) -> StreamWriteCommitMessage:
        import requests as req
        from pyspark import TaskContext

        context = TaskContext.get()
        partition_id = context.partitionId() if context else 0

        batch: list[dict] = []
        rows_sent = 0

        for row in iterator:
            batch.append(row.asDict())
            if len(batch) >= self.batch_size:
                headers = self.headers
                if self.oauth_config is not None:
                    headers = self.oauth_config.apply_to_headers(headers)
                assert self.url is not None
                resp = req.post(self.url, json=batch, headers=headers, timeout=30)
                resp.raise_for_status()
                rows_sent += len(batch)
                batch = []

        if batch:
            headers = self.headers
            if self.oauth_config is not None:
                headers = self.oauth_config.apply_to_headers(headers)
            assert self.url is not None
            resp = req.post(self.url, json=batch, headers=headers, timeout=30)
            resp.raise_for_status()
            rows_sent += len(batch)

        return StreamWriteCommitMessage(
            partition_id=partition_id, rows_sent=rows_sent, success=True
        )

    def commit(self, messages: list, batchId: int) -> None:
        total = sum(m.rows_sent for m in messages if m is not None)
        print(f"[restapi_stream_sink] batch {batchId}: committed {total} rows")

    def abort(self, messages: list, batchId: int) -> None:
        print(f"[restapi_stream_sink] batch {batchId}: aborted")
