# PySpark Custom Data Source — Copilot Instructions

## Project Overview

Reference implementation of the **PySpark 4 Python Data Source API**
(`pyspark.sql.datasource`), announced GA for Apache Spark 4.0. It provides a reusable
`custom_ds` library — batch readers, a batch sink writer, and a streaming source — plus
runnable examples demonstrating registration, batch read/write, streaming, and SQL access.
Every example is self-contained and runs locally with `local[*]`, no cluster required.

## Tech Stack

- **Python** ≥ 3.11
- **PySpark** ≥ 4.0.0 (tested against 4.2.0)
- **PyArrow** ≥ 14.0.0 — required runtime dependency of the Python Data Source API
- **Java** 17 (LTS)
- **Build**: hatchling (`pyproject.toml`, `hatchling.build` backend)
- **Package manager**: uv
- **Testing**: pytest, `pythonpath = ["src"]`, `testpaths = ["tests"]`

## Source Structure

```
src/custom_ds/
├── session.py                       # create_spark_session() — shared SparkSession helper
├── batch/
│   └── simple_source.py             # SimpleDataSource + SimpleDataSourceReader (partitioned batch read)
├── writer/
│   └── simple_writer.py             # SimpleSinkDataSource + DataSourceWriter (JSON-lines sink)
├── streaming/
│   └── simple_stream_source.py      # SimpleStreamDataSource + SimpleDataSourceStreamReader
└── util/
    └── registration.py              # register_all(spark) — registers every source in one call

examples/
├── 01_batch_read/    # spark.read.format("simple")...load()
├── 02_batch_write/   # df.write.format("simple_sink")...save()
├── 03_streaming/     # spark.readStream.format("simple_stream")...load()
└── 04_sql/           # createOrReplaceTempView() + spark.sql(...)

tests/
├── conftest.py                # session-scoped `spark` fixture
├── test_simple_source.py
└── test_simple_sink.py
```

## Modular Instruction Files

| File | Scope | Purpose |
|------|-------|---------|
| `instructions/python.instructions.md` | `**/*.py` | Python style, imports, type hints, naming |
| `instructions/pyspark-custom-ds.instructions.md` | `src/**/*.py`, `examples/**/*.py` | Python Data Source API patterns (batch reader/writer, streaming, registration) |
| `instructions/examples.instructions.md` | `examples/**/*.py` | Example script structure and required boilerplate |
| `instructions/testing.instructions.md` | `**/test_*.py`, `**/*_test.py` | pytest conventions and fixtures |
| `instructions/project-config.instructions.md` | `pyproject.toml` | Build system, dependencies, uv usage |

## Things to Avoid

- Do not implement DSv1/DSv2 Java/Scala interfaces — this project is pure Python.
- Do not omit `pyarrow` as a dependency — batch and streaming reads/writes fail without it.
- Do not return generators or other non-picklable objects from `partitions()`, `read()`, or
  offset dictionaries — Spark must serialize these across the JVM/Python boundary.
- Do not implement both `streamReader()` and `simpleStreamReader()` on the same `DataSource` —
  pick one based on whether partitioned parallel reads are needed.
- Do not hardcode absolute file paths — accept options (e.g. `path`) or use environment
  variables with `/tmp/...` fallbacks.
- Do not skip `spark.stop()` (or `query.stop()` for streaming) at the end of scripts and tests.
- Do not use `requirements.txt`, `pip install`, `poetry`, or `conda` — `pyproject.toml` + `uv`
  is the single source of truth.
