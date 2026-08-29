# Batch Reader — `RestApiDataSource`

Read JSON data from REST API endpoints into Spark DataFrames with configurable
partitioning for parallel processing.

!!! abstract "API Base Class"
    Extends [`DataSource`](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/api/pyspark.sql.datasource.DataSource.html)
    and [`DataSourceReader`](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/api/pyspark.sql.datasource.DataSourceReader.html)
    from `pyspark.sql.datasource`.

## Format Name

```python
spark.read.format("restapi")
```

## Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `url` | str | Required* | The HTTP endpoint URL |
| `urls` | str | — | Comma-separated URLs (for `urls` strategy) |
| `method` | str | `GET` | HTTP method |
| `resultKey` | str | — | Dot-path to JSON array (e.g. `"data"`, `"results.items"`) |
| `schema` | str | auto-inferred | DDL schema string |
| `partitionStrategy` | str | `single` | `"single"`, `"urls"`, or `"pages"` |
| `totalPages` | int | `1` | Number of pages (for `pages` strategy) |
| `pageSize` | int | `100` | Items per page |
| `pageParam` | str | `page` | Query param name for page number |
| `pageSizeParam` | str | `limit` | Query param name for page size |
| `headers.<name>` | str | — | Custom HTTP headers |
| `params.<name>` | str | — | Query parameters |
| `apiKey` | str | — | API key (sent via header) |
| `apiKeyHeader` | str | `X-API-Key` | Header name for API key |
| `timeout` | int | `30` | Request timeout in seconds |

*Either `url` or `urls` is required depending on strategy.

## Schema Inference

If no `schema` option is provided, the data source makes a sample request to the endpoint
and infers the schema from the first record:

- `int` → `LongType`
- `float` → `DoubleType`
- `bool` → `BooleanType`
- everything else → `StringType`
- nested dicts/lists → serialized as JSON `StringType`

!!! tip "Explicit schema is faster"
    Providing a `schema` option skips the inference HTTP call — use it in production.

## Partitioning Strategies

### Single (default)

One partition, one HTTP call:

```python
df = spark.read.format("restapi") \
    .option("url", "http://api.example.com/data") \
    .option("resultKey", "items") \
    .load()
```

### URL-based

One Spark task per URL — processed in parallel:

```python
df = spark.read.format("restapi") \
    .option("partitionStrategy", "urls") \
    .option("urls", "http://api/users/1,http://api/users/2,http://api/users/3") \
    .option("schema", "id LONG, name STRING, email STRING") \
    .load()
```

### Page-based

One Spark task per page — fetched concurrently:

```python
df = spark.read.format("restapi") \
    .option("partitionStrategy", "pages") \
    .option("url", "http://api.example.com/posts") \
    .option("totalPages", "10") \
    .option("pageSize", "50") \
    .option("resultKey", "data") \
    .option("schema", "id LONG, title STRING, author STRING") \
    .load()
```

## Result Key Navigation

The `resultKey` option supports dot-notation to navigate nested responses:

```json
{
  "meta": {"status": "ok"},
  "response": {
    "items": [{"id": 1}, {"id": 2}]
  }
}
```

```python
.option("resultKey", "response.items")
```

## Implementation

```python title="src/custom_ds/restapi/rest_source.py (simplified)"
class RestApiDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "restapi"

    def schema(self) -> str | StructType:
        # Infer from API or return user-provided schema
        ...

    def reader(self, schema: StructType) -> DataSourceReader:
        return RestApiDataSourceReader(schema, self.options)

class RestApiDataSourceReader(DataSourceReader):
    def partitions(self) -> list[InputPartition]:
        # Returns 1, N (urls), or N (pages) partitions
        ...

    def read(self, partition):
        import requests as req  # (1)!
        response = req.request(...)
        for record in records:
            yield tuple(...)
```

1. Imported inside `read()` for pickle serialization across the JVM boundary.
