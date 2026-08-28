---
applyTo: "{src/**/*.py,examples/**/*.py}"
---

# PySpark 4 Python Data Source API Patterns

## Scope

`custom_ds` is a library of reference implementations of the PySpark 4
[Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html)
(`pyspark.sql.datasource`). It targets **PySpark 4.x only** — do not write code that depends on
DSv1/DSv2 Java/Scala interfaces or `findspark`.

## SparkSession

Every standalone script uses the shared helper instead of duplicating boilerplate:

```python
from custom_ds import create_spark_session

spark = create_spark_session("my-example")
# ... work ...
spark.stop()
```

`create_spark_session()` reads `SPARK_MASTER` (default `local[*]`) and sets the log level to WARN.

## Implementing a Batch Data Source

Subclass `DataSource`, `DataSourceReader`, and (optionally) a dataclass `InputPartition`:

```python
from dataclasses import dataclass
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType


@dataclass
class RangePartition(InputPartition):
    start: int
    end: int


class MyDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "my_source"          # used in .format("my_source")

    def schema(self) -> str:
        return "id LONG, value STRING"   # DDL string or StructType; self.options is available here

    def reader(self, schema: StructType) -> DataSourceReader:
        return MyDataSourceReader(schema, self.options)


class MyDataSourceReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict) -> None:
        self.options = options

    def partitions(self) -> list[InputPartition]:
        return [RangePartition(0, 5), RangePartition(5, 10)]  # one task per partition

    def read(self, partition: RangePartition):
        for i in range(partition.start, partition.end):
            yield (i, f"row-{i}")   # tuples, Rows, or pyarrow.RecordBatch
```

### Rules

- `DataSource.__init__` is provided by the base class and stores constructor kwargs on
  `self.options` (a `CaseInsensitiveDict`) — never override `__init__` on `DataSource` itself.
- `schema()` takes no arguments; read `self.options` directly if the schema depends on options.
- Every `InputPartition` (and any offset `dict`) must be **plain and picklable** — no open file
  handles, sockets, generators, or lambdas. Generators returned mid-object will raise
  `TypeError: cannot pickle 'generator' object` once Spark tries to serialize them.
- Prefer distributing rows evenly across partitions with `divmod(total, num_partitions)` rather
  than a fixed chunk size, which silently produces an extra remainder partition.
- Register before use: `spark.dataSource.register(MyDataSource)`, then
  `spark.read.format("my_source").option(...).load()`.

## Implementing a Batch Sink (Writer)

```python
from dataclasses import dataclass
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceWriter, WriterCommitMessage


@dataclass
class MyCommitMessage(WriterCommitMessage):
    num_rows: int


class MySinkDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "my_sink"

    def writer(self, schema, overwrite: bool) -> DataSourceWriter:
        return MySinkWriter(self.options, overwrite)


class MySinkWriter(DataSourceWriter):
    def write(self, iterator) -> MyCommitMessage:
        count = 0
        for row in iterator:      # runs once per executor task
            ...
            count += 1
        return MyCommitMessage(num_rows=count)

    def commit(self, messages: list) -> None:
        ...   # runs on the driver after every task succeeds

    def abort(self, messages: list) -> None:
        ...   # runs on the driver if any task fails; clean up partial writes here
```

Register and use with `df.write.format("my_sink").option("path", ...).mode("append").save()`.

## Implementing a Streaming Source

Two APIs exist — **use the right one, they are not interchangeable**:

| Base class | `DataSource` hook | When to use |
|---|---|---|
| `SimpleDataSourceStreamReader` | `simpleStreamReader(schema)` | Lightweight sources; offset planning and reading happen together on the driver. |
| `DataSourceStreamReader` | `streamReader(schema)` | High-throughput sources needing explicit `partitions(start, end)` for parallel reads. |

```python
from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader


class MyStreamDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "my_stream"

    def schema(self) -> str:
        return "value LONG"

    def simpleStreamReader(self, schema) -> SimpleDataSourceStreamReader:
        return MyStreamReader(self.options)


class MyStreamReader(SimpleDataSourceStreamReader):
    def initialOffset(self) -> dict:
        return {"offset": 0}

    def read(self, start: dict):
        end = start["offset"] + 5
        rows = [(i,) for i in range(start["offset"], end)]   # a list, never a generator
        return iter(rows), {"offset": end}

    def readBetweenOffsets(self, start: dict, end: dict):
        return iter([(i,) for i in range(start["offset"], end["offset"])])
```

Common pitfall: overriding `streamReader()` when subclassing `SimpleDataSourceStreamReader` fails
at runtime with `AttributeError: ... has no attribute 'latestOffset'` — always pair
`SimpleDataSourceStreamReader` with the `simpleStreamReader()` hook.

Run with `spark.readStream.format("my_stream").load()` and a `writeStream` sink
(`console`, `memory`, or another custom `DataSourceStreamWriter`).

## Registration Helper

Register every library source in one call via `custom_ds.util.registration.register_all`:

```python
from custom_ds import register_all

register_all(spark)   # registers all DataSource subclasses in ALL_DATA_SOURCES
```

Add new sources to `ALL_DATA_SOURCES` in `src/custom_ds/util/registration.py` when introducing them.

## SQL Access

Any DataFrame backed by a custom data source works with plain Spark SQL once exposed as a view:

```python
spark.read.format("my_source").load().createOrReplaceTempView("my_view")
spark.sql("SELECT * FROM my_view WHERE id > 10").show()
```

## Things to Avoid

- Do not implement raw DSv1/DSv2 Java/Scala interfaces — this project is Python-only.
- Do not return generators, open file handles, or non-picklable objects from `partitions()`,
  `read()`, or offset dictionaries.
- Do not mix `streamReader()` and `simpleStreamReader()` on the same `DataSource` — implement one.
- Do not forget `pyarrow` — it is a hard runtime dependency of the Python Data Source API.
- Do not hardcode absolute file paths in sinks — accept a `path` option and use `pathlib.Path`.
- Do not skip `spark.stop()` at the end of standalone example scripts.
