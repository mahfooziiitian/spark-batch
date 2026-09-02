"""REST API batch data source — read data from an HTTP endpoint into Spark.

Demonstrates building a real-world custom connector using the Python Data Source API
that fetches JSON data from any REST endpoint. Supports:
    - Configurable URL, HTTP method, headers, and query parameters
    - API key authentication (header-based)
    - OAuth2 authentication (client_credentials, password, bearer token)
    - Automatic JSON array → rows conversion
    - Configurable result key path for nested responses (e.g. "data.items")
    - 3 partitioning strategies: single, urls, pages

Usage:
    spark.dataSource.register(RestApiDataSource)

    # Single partition (default)
    df = spark.read.format("restapi") \\
        .option("url", "http://localhost:8000/api/users") \\
        .option("resultKey", "data") \\
        .load()

    # With OAuth2 client credentials
    df = spark.read.format("restapi") \\
        .option("url", "http://localhost:8000/api/users") \\
        .option("auth", "oauth2") \\
        .option("oauth.tokenUrl", "http://auth/token") \\
        .option("oauth.clientId", "my-client") \\
        .option("oauth.clientSecret", "my-secret") \\
        .load()

    # URL-based partitioning (one partition per URL, parallel)
    df = spark.read.format("restapi") \\
        .option("partitionStrategy", "urls") \\
        .option("urls", "http://api/users/1,http://api/users/2,http://api/users/3") \\
        .load()

    # Page-based partitioning (one partition per page, parallel)
    df = spark.read.format("restapi") \\
        .option("partitionStrategy", "pages") \\
        .option("url", "http://localhost:8000/api/posts") \\
        .option("totalPages", "4") \\
        .option("pageSize", "25") \\
        .load()
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass

import requests
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from custom_ds.util.log import get_logger

logger = get_logger(__name__)
# ---------------------------------------------------------------------------
# Partition types
# ---------------------------------------------------------------------------


@dataclass
class RestApiPartition(InputPartition):
    """Standard partition: single URL + request config."""

    url: str
    method: str
    headers: dict
    params: dict
    result_key: str | None


@dataclass
class RestApiPagePartition(InputPartition):
    """Page-based partition: appends page/limit params to the base URL."""

    base_url: str
    page: int
    page_size: int
    method: str
    headers: dict
    params: dict
    result_key: str | None


# ---------------------------------------------------------------------------
# DataSource
# ---------------------------------------------------------------------------


class RestApiDataSource(DataSource):
    """A batch data source that reads JSON data from REST API endpoints.

    Options:
        url (str): The HTTP endpoint URL. Required unless using `urls` or UC connection.
        urls (str): Comma-separated URLs for URL-based partitioning.
        method (str): HTTP method (GET, POST). Default "GET".
        resultKey (str): Dot-path to the JSON array in the response.
        partitionStrategy (str): "single" (default), "urls", or "pages".
        totalPages (int): Number of pages for page-based partitioning. Default 1.
        pageSize (int): Items per page for page-based partitioning. Default 100.
        pageParam (str): Query parameter name for page number. Default "page".
        pageSizeParam (str): Query parameter name for page size. Default "limit".
        headers.<name> (str): Custom headers to include in the request.
        params.<name> (str): Query parameters to include in the request.
        apiKey (str): API key value (sent via header).
        apiKeyHeader (str): Header name for the API key. Default "X-API-Key".
        auth (str): Authentication type. Set to "oauth2" for OAuth2.
        oauth.tokenUrl (str): OAuth2 token endpoint URL.
        oauth.clientId (str): OAuth2 client ID.
        oauth.clientSecret (str): OAuth2 client secret.
        oauth.grantType (str): OAuth2 grant type. Default "client_credentials".
        oauth.scope (str): OAuth2 scope (space-separated).
        oauth.username (str): Username for "password" grant type.
        oauth.password (str): Password for "password" grant type.
        oauth.bearerToken (str): Pre-obtained bearer token (skips token fetch).
        databricks.connection (str): Unity Catalog HTTP connection name (DBR 18.1+).
            Injects host, base_path, bearer_token automatically.
        uc.host (str): UC connection host (local testing).
        uc.basePath (str): UC connection base path (local testing).
        uc.bearerToken (str): UC connection bearer token (local testing).
        uc.path (str): Additional path appended to base URL.
        timeout (int): Request timeout in seconds. Default 30.
        schema (str): DDL schema string. If not provided, inferred from response.
    """

    @classmethod
    def name(cls) -> str:
        return "restapi"

    def schema(self) -> str | StructType:
        user_schema = self.options.get("schema")
        if user_schema:
            return user_schema

        # Resolve UC HTTP connection if configured
        from custom_ds.restapi.uc_connection import UCConnectionConfig

        uc_config = UCConnectionConfig.from_options(self.options)

        # Determine the URL for schema inference
        url = self.options.get("url")
        if not url and uc_config is not None:
            uc_path = self.options.get("uc.path") or self.options.get("uc.Path") or ""
            url = uc_config.resolve_url(None, uc_path)
        if not url:
            urls_str = self.options.get("urls") or self.options.get("Urls") or ""
            url_list = [u.strip() for u in urls_str.split(",") if u.strip()]
            url = url_list[0] if url_list else None
        if not url:
            raise ValueError("Option 'url', 'urls', or 'databricks.connection' is required")

        method = self.options.get("method", "GET").upper()
        headers = _extract_prefixed_options(self.options, "headers.")
        params = _extract_prefixed_options(self.options, "params.")
        _apply_api_key(self.options, headers)
        timeout = int(self.options.get("timeout") or 30)

        # Apply UC connection auth
        if uc_config is not None:
            headers = uc_config.apply_auth_headers(headers)

        # Apply OAuth2 on the driver for schema inference
        from custom_ds.restapi.oauth import OAuth2Config

        oauth_config = OAuth2Config.from_options(self.options)
        if oauth_config is not None:
            headers = oauth_config.apply_to_headers(headers)

        logger.debug("Schema inference: %s %s params=%s", method, url, params)
        response = requests.request(method, url, headers=headers, params=params, timeout=timeout)
        logger.debug("Schema inference response: %d %s", response.status_code, response.reason)
        response.raise_for_status()
        data = response.json()

        result_key = self.options.get("resultKey") or self.options.get("resultkey")
        records = _navigate_result_key(data, result_key)

        if not records:
            return "value STRING"

        return _infer_schema(records[0])

    def reader(self, schema: StructType) -> DataSourceReader:
        return RestApiDataSourceReader(schema, self.options)


# ---------------------------------------------------------------------------
# DataSourceReader
# ---------------------------------------------------------------------------


class RestApiDataSourceReader(DataSourceReader):
    def __init__(self, schema: StructType, options: Mapping[str, str]) -> None:
        self.schema = schema
        self.options = options
        self.method = options.get("method", "GET").upper()
        self.headers = _extract_prefixed_options(options, "headers.")
        self.params = _extract_prefixed_options(options, "params.")
        self.result_key = options.get("resultKey") or options.get("resultkey")
        self.timeout = int(options.get("timeout") or 30)
        _apply_api_key(options, self.headers)

        # UC HTTP connection (pickle-safe dataclass)
        from custom_ds.restapi.uc_connection import UCConnectionConfig

        self.uc_config = UCConnectionConfig.from_options(options)

        # Resolve URL from UC connection if no explicit url
        self.url = options.get("url")
        if not self.url and self.uc_config is not None:
            uc_path = options.get("uc.path") or options.get("uc.Path") or ""
            self.url = self.uc_config.resolve_url(None, uc_path)
            # Apply UC bearer token to headers
            self.headers = self.uc_config.apply_auth_headers(self.headers)

        # OAuth2 config (pickle-safe dataclass)
        from custom_ds.restapi.oauth import OAuth2Config

        self.oauth_config = OAuth2Config.from_options(options)

        # Partitioning config
        self.strategy = (
            options.get("partitionStrategy") or options.get("partitionstrategy") or "single"
        ).lower()
        self.urls_str = options.get("urls") or options.get("Urls") or ""
        self.total_pages = int(options.get("totalPages") or options.get("totalpages") or 1)
        self.page_size = int(options.get("pageSize") or options.get("pagesize") or 100)
        self.page_param = options.get("pageParam") or options.get("pageparam") or "page"
        self.page_size_param = (
            options.get("pageSizeParam") or options.get("pagesizeparam") or "limit"
        )

        # Validate
        if self.strategy == "urls":
            if not self.urls_str:
                raise ValueError("Option 'urls' is required when partitionStrategy='urls'")
        elif not self.url:
            raise ValueError("Option 'url', 'databricks.connection', or 'uc.host' is required")

    def partitions(self) -> list[InputPartition]:
        if self.strategy == "urls":
            url_list = [u.strip() for u in self.urls_str.split(",") if u.strip()]
            return [
                RestApiPartition(
                    url=url,
                    method=self.method,
                    headers=self.headers,
                    params=self.params,
                    result_key=self.result_key,
                )
                for url in url_list
            ]

        if self.strategy == "pages":
            assert self.url is not None
            return [
                RestApiPagePartition(
                    base_url=self.url,
                    page=page,
                    page_size=self.page_size,
                    method=self.method,
                    headers=self.headers,
                    params=self.params,
                    result_key=self.result_key,
                )
                for page in range(1, self.total_pages + 1)
            ]

        # Default: single partition
        assert self.url is not None
        return [
            RestApiPartition(
                url=self.url,
                method=self.method,
                headers=self.headers,
                params=self.params,
                result_key=self.result_key,
            )
        ]

    def read(self, partition: InputPartition):
        import requests as req

        from custom_ds.util.log import get_logger as _get_logger

        _log = _get_logger(__name__)

        if isinstance(partition, RestApiPagePartition):
            page_params = {
                self.page_param: partition.page,
                self.page_size_param: partition.page_size,
            }
            merged_params = {**partition.params, **page_params}
            headers = partition.headers
            if self.oauth_config is not None:
                headers = self.oauth_config.apply_to_headers(headers)
            _log.debug("Read page %d: %s %s", partition.page, partition.method, partition.base_url)
            response = req.request(
                partition.method,
                partition.base_url,
                headers=headers,
                params=merged_params,
                timeout=self.timeout,
            )
        else:
            assert isinstance(partition, RestApiPartition)
            headers = partition.headers
            if self.oauth_config is not None:
                headers = self.oauth_config.apply_to_headers(headers)
            _log.debug("Read: %s %s", partition.method, partition.url)
            response = req.request(
                partition.method,
                partition.url,
                headers=headers,
                params=partition.params,
                timeout=self.timeout,
            )

        _log.debug(
            "Response: %d %s (%d bytes)",
            response.status_code,
            response.reason,
            len(response.content),
        )
        response.raise_for_status()
        data = response.json()

        result_key = getattr(partition, "result_key", None) or self.result_key
        records = _navigate_result_key(data, result_key)

        field_names = [f.name for f in self.schema.fields]
        for record in records:
            if isinstance(record, dict):
                yield tuple(_coerce_value(record.get(name)) for name in field_names)
            else:
                yield (str(record),) + (None,) * (len(field_names) - 1)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _coerce_value(value):
    """Coerce complex values (dicts, lists) to JSON strings for Spark."""
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


def _extract_prefixed_options(options: Mapping[str, str], prefix: str) -> dict[str, str]:
    """Extract options with a given prefix, stripping the prefix from keys."""
    result = {}
    for key, value in options.items():
        if key.lower().startswith(prefix.lower()):
            clean_key = key[len(prefix) :]
            result[clean_key] = value
    return result


def _apply_api_key(options: Mapping[str, str], headers: dict[str, str]) -> None:
    """If apiKey option is present, add it to the headers dict."""
    api_key = options.get("apiKey") or options.get("apikey")
    if api_key:
        header_name = options.get("apiKeyHeader") or options.get("apikeyheader") or "X-API-Key"
        headers[header_name] = api_key


def _navigate_result_key(data, result_key: str | None) -> list:
    """Navigate a dot-path key into the JSON response to find the records array."""
    if result_key is None:
        return data if isinstance(data, list) else [data]

    for key in result_key.split("."):
        if isinstance(data, dict):
            data = data.get(key, [])
        else:
            return []
    return data if isinstance(data, list) else [data]


def _infer_schema(record: dict) -> StructType:
    """Infer a StructType from a single JSON record (best-effort)."""
    fields = []
    for key, value in record.items():
        if isinstance(value, bool):
            from pyspark.sql.types import BooleanType

            fields.append(StructField(key, BooleanType(), True))
        elif isinstance(value, int):
            fields.append(StructField(key, LongType(), True))
        elif isinstance(value, float):
            from pyspark.sql.types import DoubleType

            fields.append(StructField(key, DoubleType(), True))
        else:
            fields.append(StructField(key, StringType(), True))
    return StructType(fields)
