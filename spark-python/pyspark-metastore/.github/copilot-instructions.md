# GitHub Copilot Instructions — pyspark-metastore

## Project Overview

This is a **PySpark Metastore reference project** demonstrating how to configure and use
different catalog and metastore backends with Apache Spark. It covers in-memory, Hive,
Glue, Iceberg, Delta Lake, JDBC, REST, Hadoop, Unity Catalog, and custom catalog
implementations.

## Repository Layout

```
pyspark-metastore/
├── src/
│   ├── catalog/
│   │   ├── namespace/          # Three-level namespace resolution examples
│   │   └── non_persistent/     # Temp views, non-persistent catalog
│   ├── metastore/
│   │   ├── catalog_metadata.py # Shared introspection helpers
│   │   ├── memory/             # In-memory (default) metastore
│   │   ├── spark/              # Spark built-in catalog
│   │   ├── hive/               # Hive Metastore (remote, server, LLAP)
│   │   ├── glue/               # AWS Glue Data Catalog
│   │   ├── iceberg/            # Iceberg catalogs (Hive, Hadoop, REST)
│   │   ├── delta_lake/         # Delta Lake catalog
│   │   ├── external/           # External RDBMS-backed metastore
│   │   ├── jdbc_metastore/     # JDBC catalog (DataSource V2)
│   │   ├── rest/               # REST catalog
│   │   ├── multi_catalog/      # Multi-catalog setup
│   │   ├── custom/             # Custom TableCatalog implementation
│   │   ├── haroop_catalog/     # Hadoop (Iceberg) catalog
│   │   └── dbx_uc/             # Databricks Unity Catalog
│   └── warehouse/              # spark.sql.warehouse.dir examples
├── tests/
├── docs/
│   ├── index.md
│   ├── README.md               # Metastore concepts overview
│   ├── metastore/              # Per-catalog documentation
│   └── warehouse/              # Warehouse directory docs
└── pyproject.toml
```

## Tech Stack & Versions

| Component | Version |
|-----------|---------|
| Apache Spark / PySpark | 3.5.x (< 4.0.0) |
| Python | 3.11 |
| Java | 11 (LTS) |
| Documentation | MkDocs Material ≥ 9.5 |
| Testing | pytest 8.x + chispa |
| Linting | ruff, flake8, mypy, SQLFluff (Databricks dialect) |
| Package management | uv |
| Task runner | taskipy |

## General Conventions

- **No boilerplate comments.** Only comment code that needs clarification.
- Prefer `SPARK_MASTER` env var with `local[*]` fallback.
- Input/output paths always come from environment variables with `/tmp/...` fallbacks.
- `spark.sparkContext.setLogLevel("WARN")` in example scripts; `"ERROR"` in tests.
- Use `from pyspark.sql import functions as F` — never `import *`.
- Always call `spark.stop()` at the end of standalone scripts.
- Never hard-code credentials — load from env vars.
- Three-level namespace (`catalog.database.table`) is preferred in examples.

## SparkSession — Catalog-Aware Pattern

```python
import os
from pyspark.sql import SparkSession

warehouse_dir = os.environ.get("SPARK_WAREHOUSE", "/tmp/spark-warehouse")

spark = (SparkSession.builder
         .appName("metastore-job")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.warehouse.dir", warehouse_dir)
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

## Task Runner

```bash
uv run task test            # pytest
uv run task quality         # isort + ruff + flake8 + mypy + sqlfluff
uv run task secure          # bandit + safety
uv run task sql_lint        # sqlfluff lint (Databricks dialect)
uv run task sql_format      # sqlfluff fix
```
