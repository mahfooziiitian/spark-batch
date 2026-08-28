# PySpark Custom Data Source

Build custom Spark connectors in **pure Python** using the PySpark 4 Python Data Source API
(`pyspark.sql.datasource`). No JVM, no Scala — just Python.

---

## What is this?

A reference implementation library (`custom_ds`) demonstrating the full Python Data Source API:

| Capability | Source (Read) | Sink (Write) |
|---|---|---|
| **Batch** | `RestApiDataSource`, `RestApiArrowDataSource` | `RestApiSinkDataSource` |
| **Streaming** | `RestApiStreamDataSource` | `RestApiStreamSinkDataSource` |

Plus simple in-memory sources (`SimpleDataSource`, `SimpleSinkDataSource`, `SimpleStreamDataSource`)
for learning the API without external dependencies.

---

## Architecture

```mermaid
graph TB
    subgraph "PySpark 4 Python Data Source API"
        DS[DataSource] --> R[DataSourceReader]
        DS --> W[DataSourceWriter]
        DS --> SR[SimpleDataSourceStreamReader]
        DS --> SW[DataSourceStreamWriter]
    end

    subgraph "custom_ds Library"
        REST[RestApiDataSource] --> |GET| API[(REST API)]
        SINK[RestApiSinkDataSource] --> |POST| API
        STREAM[RestApiStreamDataSource] --> |poll| API
        ARROW[RestApiArrowDataSource] --> |GET + Arrow| API
    end

    R --> REST
    W --> SINK
    SR --> STREAM
    R --> ARROW
```

---

## Key Features

- :fontawesome-brands-python: **Pure Python** — no JVM connector development needed
- :material-rocket-launch: **3 Partitioning Strategies** — single, URL-based, page-based
- :material-arrow-right-bold: **Arrow Batch Support** — up to 10x throughput for large payloads
- :material-stream: **Streaming** — poll endpoints with offset tracking
- :material-database: **SQL Access** — `createOrReplaceTempView` + Spark SQL
- :material-shield-check: **API Key Auth** — header-based authentication

---

## Quick Example

```python
from custom_ds import create_spark_session, RestApiDataSource

spark = create_spark_session("demo")
spark.dataSource.register(RestApiDataSource)

df = spark.read.format("restapi") \
    .option("url", "https://jsonplaceholder.typicode.com/users") \
    .load()

df.show()
spark.stop()
```

---

## Next Steps

- [Installation](getting-started/installation.md) — set up the project
- [Quick Start](getting-started/quickstart.md) — run your first example
- [API Reference](api/index.md) — detailed DataSource documentation
- [Examples](examples/index.md) — runnable demos with mock servers
