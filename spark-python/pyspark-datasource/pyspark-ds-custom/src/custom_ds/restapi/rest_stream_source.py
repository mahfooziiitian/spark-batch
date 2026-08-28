"""REST API streaming data source — polls an HTTP endpoint for new records.

Demonstrates implementing SimpleDataSourceStreamReader for REST APIs. Tracks
an integer offset and fetches only new records each micro-batch by passing
``since_id`` (or a configurable param) to the endpoint.

Usage:
    spark.dataSource.register(RestApiStreamDataSource)

    df = spark.readStream.format("restapi_stream") \\
        .option("url", "http://localhost:8000/api/events") \\
        .option("offsetParam", "since_id") \\
        .option("offsetKey", "id") \\
        .load()
"""

from __future__ import annotations

from collections.abc import Iterator

from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader
from pyspark.sql.types import LongType, StringType, StructField, StructType


class RestApiStreamDataSource(DataSource):
    """A streaming source that polls a REST API endpoint for new records.

    Options:
        url (str): Required. The HTTP endpoint to poll.
        offsetParam (str): Query parameter name to send the current offset. Default "since_id".
        offsetKey (str): JSON field in each record that represents the monotonic offset. Default "id".
        limit (int): Max records per poll. Default 100.
        headers.<name> (str): Custom headers.
        apiKey (str): API key value.
        apiKeyHeader (str): Header name for API key. Default "X-API-Key".
        schema (str): DDL schema string. Required for streaming (no auto-inference).
    """

    @classmethod
    def name(cls) -> str:
        return "restapi_stream"

    def schema(self) -> str | StructType:
        user_schema = self.options.get("schema")
        if user_schema:
            return user_schema
        # Default schema for demo purposes
        return StructType(
            [
                StructField("id", LongType(), False),
                StructField("event", StringType(), True),
                StructField("timestamp", StringType(), True),
            ]
        )

    def simpleStreamReader(self, schema: StructType) -> SimpleDataSourceStreamReader:
        return RestApiStreamReader(schema, self.options)


class RestApiStreamReader(SimpleDataSourceStreamReader):
    def __init__(self, schema: StructType, options: dict) -> None:
        self.schema = schema
        self.url = options.get("url")
        if not self.url:
            raise ValueError("Option 'url' is required for the restapi_stream data source")
        self.offset_param = options.get("offsetParam") or options.get("offsetparam") or "since_id"
        self.offset_key = options.get("offsetKey") or options.get("offsetkey") or "id"
        self.limit = int(options.get("limit") or 100)
        self.result_key = options.get("resultKey") or options.get("resultkey")
        self.headers: dict = {}
        for key, value in options.items():
            if key.lower().startswith("headers."):
                self.headers[key[8:]] = value
        api_key = options.get("apiKey") or options.get("apikey")
        if api_key:
            header_name = options.get("apiKeyHeader") or options.get("apikeyheader") or "X-API-Key"
            self.headers[header_name] = api_key
        self.field_names = [f.name for f in schema.fields]

    def initialOffset(self) -> dict:
        return {"offset": 0}

    def read(self, start: dict) -> tuple[Iterator[tuple], dict]:
        current_offset = start["offset"]
        records = self._fetch(current_offset)

        if not records:
            return iter([]), start

        rows = [tuple(record.get(name) for name in self.field_names) for record in records]
        # Advance offset to max id seen
        max_offset = max(int(record.get(self.offset_key, current_offset)) for record in records)
        return iter(rows), {"offset": max_offset}

    def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[tuple]:
        records = self._fetch(start["offset"])
        end_offset = end["offset"]
        filtered = [r for r in records if int(r.get(self.offset_key, 0)) <= end_offset]
        return iter([tuple(record.get(name) for name in self.field_names) for record in filtered])

    def _fetch(self, since_offset: int) -> list[dict]:
        import requests as req

        params = {self.offset_param: since_offset, "limit": self.limit}
        try:
            response = req.get(self.url, headers=self.headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
        except req.RequestException:
            return []

        if self.result_key:
            for key in self.result_key.split("."):
                data = data.get(key, []) if isinstance(data, dict) else []
        return data if isinstance(data, list) else [data]
