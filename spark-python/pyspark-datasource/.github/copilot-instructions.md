# Copilot Instructions — pyspark-datasource

This is the **parent module** for all PySpark datasource examples. Each child
project demonstrates reading, writing, and processing data with a specific
Apache Spark datasource format. Every example is self-contained and runnable
locally — no cluster required.

## Modular Instruction Files

| File | Scope (`applyTo`) | Purpose |
|------|--------------------|---------|
| `instructions/python.instructions.md` | `**/*.py` | Python style, imports, type hints, docstrings |
| `instructions/pyspark.instructions.md` | `**/src/**/*.py` | PySpark SparkSession, DataFrame, and datasource patterns |
| `instructions/testing.instructions.md` | `**/tests/**/*.py` | pytest conventions, SparkSession fixture, assertions |
| `instructions/mkdocs.instructions.md` | `**/docs/**/*.md`, `**/mkdocs.yml` | MkDocs Material documentation style |
| `instructions/shell.instructions.md` | `**/*.sh` | Shell script conventions |
| `instructions/docker.instructions.md` | `**/Dockerfile`, `**/docker-compose*.yml` | Docker and container patterns |

## Child Projects

| Project | Datasource | Package Manager | Maturity |
|---------|-----------|-----------------|----------|
| `psyaprk-ds-parquet` | Parquet | setuptools | Stub |
| `pyspark-ds-api` | REST API → Spark | uv | Mature |
| `pyspark-ds-csv` | CSV | Poetry | Stub |
| `pyspark-ds-delta` | Delta Lake | — | Stub |
| `pyspark-ds-jdbc` | JDBC (Oracle, MySQL, MSSQL) | Poetry | Mature |
| `pyspark-ds-json` | JSON | setuptools | Mature |
| `pyspark-ds-pdf` | PDF (spark-pdf) | uv | Very Mature |
| `pyspark-ds-sequentialfile` | Hadoop SequenceFile | — | Stub |
| `pyspark-ds-text` | Text | uv | Moderate |
| `pyspark-ds-xml/` | XML (3 sub-projects) | uv | Very Mature |
| `pyspark-fhir` | FHIR (Bunsen) | — | Stub |
| `pyspark-kafka` | Kafka | Poetry | Moderate |

## Technology Stack

| Component | Version / Tool |
|-----------|---------------|
| Python | ≥ 3.11 |
| PySpark | 3.5.x |
| Java | 11 (LTS) |
| Package managers | uv (preferred), Poetry, setuptools |
| Testing | pytest ≥ 8.0 |
| Documentation | MkDocs Material ≥ 9.5 |

## Conventions

- Each child project is independently runnable with its own `pyproject.toml`.
- **uv** is the preferred package manager for new projects.
- All scripts use `SPARK_MASTER` env var with `local[*]` fallback.
- Input/output paths come from environment variables with `/tmp/...` fallbacks.
- `from pyspark.sql import functions as F` — never `import *`.
- Always call `spark.stop()` at the end of standalone scripts.
- Parquet is the preferred output format.

## Things to Avoid

- Do **not** use `from pyspark.sql.functions import *`.
- Do **not** omit `spark.stop()` in standalone scripts.
- Do **not** use `len(df.collect())` — use `df.count()` for row counts.
- Do **not** hardcode cluster connection strings — use environment variables.
- Do **not** commit secrets, credentials, or API keys to source control.
