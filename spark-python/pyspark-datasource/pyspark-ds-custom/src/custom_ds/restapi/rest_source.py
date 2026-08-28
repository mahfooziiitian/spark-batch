"""REST API batch data source — read data from an HTTP endpoint into Spark.

Demonstrates building a real-world custom connector using the Python Data Source API
that fetches JSON data from any REST endpoint. Supports:
    - Configurable URL, HTTP method, headers, and query parameters
    - API key authentication (header-based)
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
from dataclasses import dataclass

import requests
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

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
        url (str): The HTTP endpoint URL. Required unless using `urls`.
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

        # Determine the URL for schema inference
        url = self.options.get("url")
        if not url:
            urls_str = self.options.get("urls") or self.options.get("Urls") or ""
            url_list = [u.strip() for u in urls_str.split(",") if u.strip()]
            url = url_list[0] if url_list else None
        if not url:
            raise ValueError("Option 'url' or 'urls' is required for the restapi data source")

        method = self.options.get("method", "GET").upper()
        headers = _extract_prefixed_options(self.options, "headers.")
        params = _extract_prefixed_options(self.options, "params.")
        _apply_api_key(self.options, headers)
        timeout = int(self.options.get("timeout") or 30)

        response = requests.request(method, url, headers=headers, params=params, timeout=timeout)
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
    def __init__(self, schema: StructType, options: dict) -> None:
        self.schema = schema
        self.options = options
        self.method = options.get("method", "GET").upper()
        self.headers = _extract_prefixed_options(options, "headers.")
        self.params = _extract_prefixed_options(options, "params.")
        self.result_key = options.get("resultKey") or options.get("resultkey")
        self.timeout = int(options.get("timeout") or 30)
        _apply_api_key(options, self.headers)

        # Partitioning config
        self.strategy = (
            options.get("partitionStrategy") or options.get("partitionstrategy") or "single"
        ).lower()
        self.url = options.get("url")
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
            raise ValueError("Option 'url' is required for the restapi data source")

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
        return [
            RestApiPartition(
                url=self.url,
                method=self.method,
                headers=self.headers,
                params=self.params,
                result_key=self.result_key,
            )
        ]

    def read(self, partition):
        import requests as req

        if isinstance(partition, RestApiPagePartition):
            # Build URL with pagination params
            page_params = {
                self.page_param: partition.page,
                self.page_size_param: partition.page_size,
            }
            merged_params = {**partition.params, **page_params}
            response = req.request(
                partition.method,
                partition.base_url,
                headers=partition.headers,
                params=merged_params,
                timeout=self.timeout,
            )
        else:
            response = req.request(
                partition.method,
                partition.url,
                headers=partition.headers,
                params=partition.params,
                timeout=self.timeout,
            )

        response.raise_for_status()
        data = response.json()

        records = _navigate_result_key(data, partition.result_key)

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


def _extract_prefixed_options(options: dict, prefix: str) -> dict:
    """Extract options with a given prefix, stripping the prefix from keys."""
    result = {}
    for key, value in options.items():
        if key.lower().startswith(prefix.lower()):
            clean_key = key[len(prefix) :]
            result[clean_key] = value
    return result


def _apply_api_key(options: dict, headers: dict) -> None:
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
