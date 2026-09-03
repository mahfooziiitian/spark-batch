# Copilot Instructions — spark-xml-etree

This project demonstrates how to parse, extract, and build XML inside PySpark
DataFrames using Python's built-in `xml.etree.ElementTree` through Spark UDFs.
No external XML JARs are required — everything runs with the standard library.

## Modular Instruction Files

| File | Scope (`applyTo`) | Purpose |
|------|--------------------|---------|
| `instructions/python.instructions.md` | `**/*.py` | Python style, imports, type hints, docstrings |
| `instructions/pyspark-etree.instructions.md` | `src/**/*.py`, `examples/**/*.py` | PySpark + ElementTree UDF patterns |
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
| PySpark | >= 4.0.0 (Spark 4) |
| XML library | `xml.etree.ElementTree` (stdlib) |
| Package manager | uv |
| Testing | pytest ≥ 8.0 |
| Documentation | MkDocs Material ≥ 9.5 |

## Folder Conventions

| Folder | Holds | Import? |
|--------|-------|---------|
| `src/spark_etree/` | **Library / helper / utility** code — reusable, importable modules (shared UDFs, parsing helpers). | Yes — `from spark_etree import ...` |
| `examples/` | **Usage / demo** scripts — self-contained, runnable ElementTree-UDF examples. | No — run directly |

- Reusable helpers go in `src/spark_etree/`; runnable demonstrations go in
  `examples/`. Example scripts build their own SparkSession, embed sample XML
  inline, and call `spark.stop()`.

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
│   └── spark_etree/                              # library / helper / utility code
│       └── __init__.py
├── examples/                                     # runnable usage / demo scripts
│   ├── xmls_data_processing.py                   # single-field UDF
│   ├── xmls_data_processing_multiple_column.py   # struct UDF
│   └── xmls_data_processing_multiple_column2.py  # attributes + explode
├── tests/                                        # pytest suite
├── docs/                                         # MkDocs documentation
├── mkdocs.yml
├── pyproject.toml
└── README.md
```

## Quick Reference

```bash
uv sync                           # install dependencies
uv run pytest                     # run all tests
uv run pytest tests/ -v           # verbose test output
uv run python examples/xmls_data_processing.py  # run an example
```

## Things to Avoid

- Do **not** use `from pyspark.sql.functions import *` — always `import functions as F`.
- Do **not** use the `requests` library to fetch XML at runtime — embed sample data inline.
- Do **not** leave `spark.stop()` out of standalone scripts.
- Do **not** use `print(df.schema)` — use `df.printSchema()` instead.
- Do **not** register UDFs without explicit return type annotations.
