# Copilot Instructions — spark-xml-etree

This project demonstrates how to parse, extract, and build XML inside PySpark
DataFrames using Python's built-in `xml.etree.ElementTree` through Spark UDFs.
No external XML JARs are required — everything runs with the standard library.

## Modular Instruction Files

| File | Scope (`applyTo`) | Purpose |
|------|--------------------|---------|
| `instructions/python.instructions.md` | `**/*.py` | Python style, imports, type hints, docstrings |
| `instructions/pyspark-etree.instructions.md` | `src/**/*.py` | PySpark + ElementTree UDF patterns |
| `instructions/testing.instructions.md` | `tests/**/*.py` | pytest conventions, SparkSession fixture, assertions |
| `instructions/mkdocs.instructions.md` | `docs/**/*.md`, `mkdocs.yml` | MkDocs Material documentation style |
| `instructions/project-config.instructions.md` | `pyproject.toml`, `uv.lock`, `.python-version` | Package manager and project metadata |

## Project Overview

Parse XML columns stored as strings in Spark DataFrames by sending each row
through a Python UDF that calls `xml.etree.ElementTree`. This is useful when:

- The XML schema is irregular or deeply nested.
- You need namespace-aware parsing with `findall(xpath, namespaces)`.
- You want to avoid adding the Databricks spark-xml JAR to the cluster.
- You need fine-grained error handling for malformed XML.

## Technology Stack

| Component | Version / Tool |
|-----------|---------------|
| Python | ≥ 3.11 |
| PySpark | < 4.0.0 (3.5.x preferred) |
| XML library | `xml.etree.ElementTree` (stdlib) |
| Package manager | uv |
| Testing | pytest ≥ 8.0 |
| Documentation | MkDocs Material ≥ 9.5 |

## Project Structure

```
spark-xml-etree/
├── .github/
│   ├── copilot-instructions.md          ← you are here
│   └── instructions/
│       ├── python.instructions.md
│       ├── pyspark-etree.instructions.md
│       ├── testing.instructions.md
│       ├── mkdocs.instructions.md
│       └── project-config.instructions.md
├── src/
│   └── spark_etree/
│       ├── __init__.py
│       ├── xmls_data_processing.py               # single-field UDF
│       ├── xmls_data_processing_multiple_column.py  # struct UDF
│       ├── xmls_data_processing_multiple_column2.py # attributes + explode
│       ├── xmls_namespace_handling.py             # XML namespaces
│       ├── xmls_nested_flattening.py              # order → line items
│       ├── xmls_error_handling.py                 # malformed XML
│       └── xmls_build_from_dataframe.py           # DataFrame → XML
├── tests/
│   ├── __init__.py
│   ├── conftest.py                                # session-scoped SparkSession
│   ├── test_data_processing.py
│   ├── test_attributes_explode.py
│   ├── test_namespace_handling.py
│   ├── test_nested_flattening.py
│   ├── test_error_handling.py
│   └── test_build_from_dataframe.py
├── pyproject.toml
├── README.md
└── uv.lock
```

## Quick Reference

```bash
uv sync                           # install dependencies
uv run pytest                     # run all tests
uv run pytest tests/ -v           # verbose test output
uv run python src/spark_etree/xmls_data_processing.py  # run an example
```

## Things to Avoid

- Do **not** use `from pyspark.sql.functions import *` — always `import functions as F`.
- Do **not** use the `requests` library to fetch XML at runtime — embed sample data inline.
- Do **not** leave `spark.stop()` out of standalone scripts.
- Do **not** use `print(df.schema)` — use `df.printSchema()` instead.
- Do **not** register UDFs without explicit return type annotations.
