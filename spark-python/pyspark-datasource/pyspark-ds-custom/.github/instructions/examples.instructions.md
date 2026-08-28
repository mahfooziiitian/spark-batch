---
applyTo: "examples/**/*.py"
---

# Examples Instructions

## Structure

Examples are organized in numbered directories by topic, one concept per folder:

```
examples/
├── 01_batch_read/    # Registering + reading a partitioned batch DataSource
├── 02_batch_write/   # Writing via a custom DataSourceWriter sink
├── 03_streaming/     # Streaming reads with SimpleDataSourceStreamReader
└── 04_sql/           # Querying a custom data source through Spark SQL
```

## File Naming

- Use numbered prefixes for the top-level topic directory (`01_`, `02_`, …).
- File names inside each directory are descriptive `snake_case`, no numeric prefix needed
  unless a directory holds multiple related scripts.

## Required Boilerplate

Every example file must:

1. Start with a module-level docstring listing "Key concepts" covered by the script.
2. Import shared helpers from `custom_ds` (`create_spark_session`, the relevant `DataSource`
   classes, or `register_all`) — never duplicate raw `SparkSession.builder` boilerplate.
3. Register the data source(s) it uses with `spark.dataSource.register(...)` (or `register_all`).
4. Use `if __name__ == "__main__":` as the entry point guard.
5. Call `spark.stop()` at the end of the script (skip only for `awaitTermination`-bounded
   streaming queries, where `query.stop()` runs first).

```python
"""Title — short description.

Key concepts:
    - Concept 1
    - Concept 2
"""

from __future__ import annotations

from custom_ds import SimpleDataSource, create_spark_session

if __name__ == "__main__":
    spark = create_spark_session("example-name")
    spark.dataSource.register(SimpleDataSource)

    # ... example code ...

    spark.stop()
```

## Options and Paths

- Never hardcode absolute paths; read them from an environment variable with a `/tmp/...` or
  `tempfile.gettempdir()` fallback (see `examples/02_batch_write/write_simple_sink.py`).
- Pass data-source options with `.option(key, value)` / `.options(**kwargs)`, matching the
  `Options:` section documented on the corresponding `DataSource` subclass's docstring.

## Streaming Examples

- Always bound the run with `query.awaitTermination(timeout=<seconds>)` followed by
  `query.stop()` — never leave an example running indefinitely.
- Prefer the `console` sink with `outputMode("append")` for readability in a terminal.

## Output Conventions

- Use `df.show()` / `print()` for output — keep examples dependency-free (no extra logging
  frameworks).
- Print a short label before non-obvious output (e.g. `print(f"row count: {df.count()}")`).
