# GitHub Copilot Instructions — pyspark-dataframe

## Module Overview

This is the **PySpark DataFrame API** reference module within the `spark-batch` repository.
It contains runnable source examples, pytest test suites, and MkDocs documentation covering
every aspect of the Spark DataFrame API: creation, column operations, joins, window functions,
aggregations, pivoting, transformations, schema handling, and performance optimization.

## Module Layout

```
├── src/
|    ├── data_frame/
│    |   ├── creation/          # DataFrame creation from tuples, lists, dicts, JSON
│    |   ├── columns/           # Column operations, aliases, expressions
│    |   ├── joins/             # inner, outer, cross, broadcast, self, natural
│    |   │   ├── broadcast/
│    |   │   ├── cross/
│    |   │   ├── inner/
│    |   │   ├── outer/
│    |   │   └── self/
│    |   ├── analytical/        # Window functions and pivoting
│    |   │   ├── window_function/
│    |   │   │   ├── aggregate/
│    |   │   │   ├── analytical/
│    |   │   ├── ranking/
│    |   │   │   ├── specification/
│    |   │   └── pivoting/
│    |   ├── optimization/      # Caching, skew data, AQE
│    |   │   ├── caching/
│    |   │   └── skew_data/
│    |   ├── schema/            # StructType definitions, JSON schema parsing
│    |   ├── transformation/    # Filter, sort, dedup, union, sampling, partition
│    |   └── etl/               # End-to-end ETL pipeline examples
├── tests/
│   ├── conftest.py        # Shared session-scoped SparkSession fixture
│   ├── creation/
│   ├── columns/
│   ├── driver/
│   ├── schema/
│   └── transformation/
└── docs/                  # MkDocs Material pages (one per topic)
```

## Tech Stack

| Component | Version |
|-----------|---------|
| PySpark   | 3.5.x   |
| Python    | 3.11 (preferred); 3.8–3.12 supported |
| Java      | 11 (LTS) |
| pytest    | latest  |
| MkDocs Material | ≥ 9.5 |

## SparkSession Pattern

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("descriptive-job-name")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions",              "4")
         .config("spark.sql.adaptive.enabled",                "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.ui.enabled",                          "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

- Never hardcode `JAVA_HOME` or a fixed master URL in source files.
- `shuffle.partitions = "4"` for all local / example scripts.
- `spark.ui.enabled = "false"` in every script and test to skip the Web UI.

## Imports

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F        # always alias as F — never import *
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, BooleanType,
)
```

## General Conventions

- **No boilerplate comments.** Only comment code that genuinely needs clarification.
- All paths come from env vars with `/tmp/` fallbacks: `os.environ.get("OUTPUT_PATH", "/tmp/output")`.
- `spark.sparkContext.setLogLevel("WARN")` in example scripts; `"ERROR"` in tests.
- Parquet is the preferred output format; CSV only for non-technical audiences.
- Always call `spark.stop()` at the end of standalone scripts.
- Every standalone script must have an `if __name__ == "__main__":` entry point.
- Prefer method chaining over intermediate variables for transformation pipelines.

## DataFrame Creation

| Source         | Method |
|----------------|--------|
| Tuples         | `spark.createDataFrame(data, ["col1", "col2"])` |
| Dicts          | `spark.createDataFrame([{"a": 1}, {"a": 2}])` |
| Explicit schema | `spark.createDataFrame(data, schema)` with `StructType` |
| JSON string    | `spark.read.json(spark.sparkContext.parallelize([json_str]))` |

Always use `StructType` when nullability or exact types matter.

## Join Conventions

- Pass join keys as a list: `df1.join(df2, on=["key"], how="inner")`.
- Alias both sides of a self-join to avoid ambiguous column names.
- Use `F.broadcast(small_df)` to force broadcast on known-small tables.
- Supported `how` values: `inner`, `left`, `right`, `full`, `left_semi`, `left_anti`, `cross`.

## Window Function Conventions

```python
w = (Window
     .partitionBy("region")
     .orderBy("date")
     .rowsBetween(Window.unboundedPreceding, Window.currentRow))

df = df.withColumn("running_total", F.sum("revenue").over(w))
```

- Always specify `.partitionBy()` unless a global window is intentional.
- Use `rowsBetween` for count-based frames; `rangeBetween` for value-based frames.

## Test Conventions

- One session-scoped `spark` fixture in `tests/conftest.py` — never create a new session per file.
- Group tests into classes matching `src/` topic structure.
- Prefer `df.count()` over `len(df.collect())` for row-count assertions.
- Use `pytest`'s `tmp_path` fixture for all file I/O tests.
- Every test file includes `if __name__ == "__main__": pytest.main([__file__, "-v"])`.

## Documentation Conventions

- Every topic page follows: description → diagram → API table → annotated example → Run section → admonitions → full source include.
- Use `--8<-- "src/<topic>/<file>.py"` to include source files verbatim.
- Use Mermaid diagrams for join visualisations and window frame boundaries.
- Tabbed blocks (`=== "pip"` / `=== "conda"` / `=== "uv"`) for install instructions.
- Admonitions: `!!! tip`, `!!! warning`, `!!! note`, `!!! success`, `!!! failure`.

## Instruction Files

Detailed rules for each file type live in `.github/instructions/`:

| File | Applies to |
|------|-----------|
| `src.instructions.md`   | `src/**/*.py`   |
| `tests.instructions.md` | `tests/**/*.py` |
| `docs.instructions.md`  | `docs/**/*.md`  |
