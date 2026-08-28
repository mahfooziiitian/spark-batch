"""REST API batch reader with Arrow Batch support for improved performance.

Instead of yielding row tuples one-by-one, this reader yields pyarrow.RecordBatch
objects directly, avoiding per-row serialization overhead. This can provide up to
10x throughput improvement for large API responses.

Usage:
    spark.dataSource.register(RestApiArrowDataSource)

    df = spark.read.format("restapi_arrow") \\
        .option("url", "http://localhost:9090/api/users") \\
        .option("resultKey", "data") \\
        .option("schema", "id LONG, name STRING, email STRING, age LONG") \\
        .load()
"""

from __future__ import annotations

from dataclasses import dataclass

import requests
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


@dataclass
class ArrowRestPartition(InputPartition):
    """Single partition carrying the request configuration."""

    url: str
    method: str
    headers: dict
    params: dict
    result_key: str | None


class RestApiArrowDataSource(DataSource):
    """A batch data source that reads JSON from a REST API and yields Arrow batches.

    Same options as RestApiDataSource, but uses pyarrow.RecordBatch for high throughput.

    Options:
        url (str): Required. The HTTP endpoint URL.
        method (str): HTTP method. Default "GET".
        resultKey (str): Dot-path to the JSON array in the response.
        headers.<name> (str): Custom headers.
        params.<name> (str): Query parameters.
        apiKey (str): API key (sent via header).
        apiKeyHeader (str): Header name for API key. Default "X-API-Key".
        schema (str): DDL schema string. If not provided, inferred from response.
    """

    @classmethod
    def name(cls) -> str:
        return "restapi_arrow"

    def schema(self) -> str | StructType:
        user_schema = self.options.get("schema")
        if user_schema:
            return user_schema

        url = self.options.get("url")
        if not url:
            raise ValueError("Option 'url' is required for the restapi_arrow data source")

        method = self.options.get("method", "GET").upper()
        headers = _extract_prefixed_options(self.options, "headers.")
        params = _extract_prefixed_options(self.options, "params.")
        _apply_api_key(self.options, headers)

        response = requests.request(method, url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        result_key = self.options.get("resultKey") or self.options.get("resultkey")
        records = _navigate_result_key(data, result_key)

        if not records:
            return "value STRING"
        return _infer_schema(records[0])

    def reader(self, schema: StructType) -> DataSourceReader:
        return RestApiArrowReader(schema, self.options)


class RestApiArrowReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict) -> None:
        self.schema = schema
        self.url = options.get("url")
        if not self.url:
            raise ValueError("Option 'url' is required for the restapi_arrow data source")
        self.method = options.get("method", "GET").upper()
        self.headers = _extract_prefixed_options(options, "headers.")
        self.params = _extract_prefixed_options(options, "params.")
        self.result_key = options.get("resultKey") or options.get("resultkey")
        _apply_api_key(options, self.headers)

    def partitions(self) -> list[InputPartition]:
        return [
            ArrowRestPartition(
                url=self.url,
                method=self.method,
                headers=self.headers,
                params=self.params,
                result_key=self.result_key,
            )
        ]

    def read(self, partition: ArrowRestPartition):
        import pyarrow as pa
        import requests as req

        response = req.request(
            partition.method,
            partition.url,
            headers=partition.headers,
            params=partition.params,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        records = _navigate_result_key(data, partition.result_key)

        if not records:
            # Yield an empty batch matching the schema
            arrays = [
                pa.array([], type=_spark_to_arrow_type(f.dataType)) for f in self.schema.fields
            ]
            arrow_schema = pa.schema(
                [(f.name, _spark_to_arrow_type(f.dataType)) for f in self.schema.fields]
            )
            yield pa.RecordBatch.from_arrays(arrays, schema=arrow_schema)
            return

        # Build columnar arrays from the records
        field_names = [f.name for f in self.schema.fields]
        columns = {name: [] for name in field_names}
        for record in records:
            for name in field_names:
                columns[name].append(record.get(name))

        arrays = [
            pa.array(columns[f.name], type=_spark_to_arrow_type(f.dataType))
            for f in self.schema.fields
        ]
        arrow_schema = pa.schema(
            [(f.name, _spark_to_arrow_type(f.dataType)) for f in self.schema.fields]
        )
        yield pa.RecordBatch.from_arrays(arrays, schema=arrow_schema)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _spark_to_arrow_type(spark_type):
    """Map Spark types to PyArrow types."""
    import pyarrow as pa

    type_map = {
        LongType(): pa.int64(),
        StringType(): pa.string(),
        DoubleType(): pa.float64(),
        BooleanType(): pa.bool_(),
    }
    return type_map.get(spark_type, pa.string())


def _extract_prefixed_options(options: dict, prefix: str) -> dict:
    result = {}
    for key, value in options.items():
        if key.lower().startswith(prefix.lower()):
            result[key[len(prefix) :]] = value
    return result


def _apply_api_key(options: dict, headers: dict) -> None:
    api_key = options.get("apiKey") or options.get("apikey")
    if api_key:
        header_name = options.get("apiKeyHeader") or options.get("apikeyheader") or "X-API-Key"
        headers[header_name] = api_key


def _navigate_result_key(data, result_key: str | None) -> list:
    if result_key is None:
        return data if isinstance(data, list) else [data]
    for key in result_key.split("."):
        if isinstance(data, dict):
            data = data.get(key, [])
        else:
            return []
    return data if isinstance(data, list) else [data]


def _infer_schema(record: dict) -> StructType:
    fields = []
    for key, value in record.items():
        if isinstance(value, bool):
            fields.append(StructField(key, BooleanType(), True))
        elif isinstance(value, int):
            fields.append(StructField(key, LongType(), True))
        elif isinstance(value, float):
            fields.append(StructField(key, DoubleType(), True))
        else:
            fields.append(StructField(key, StringType(), True))
    return StructType(fields)
