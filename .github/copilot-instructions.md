# GitHub Copilot Instructions — spark-batch

## Project Overview

This is a **multi-language Apache Spark reference repository** covering PySpark (Python),
Spark Scala, Spark Java, Spark R, Spark SQL, and Databricks SQL. Each language lives in its
own top-level directory. The primary focus is **PySpark** under `spark-python/`.

## Repository Layout

```
spark-batch/
├── spark-python/          # PySpark — main focus
│   ├── pyspark-setup/     # Installation & environment setup per platform
│   ├── pyspark-running/   # Job execution examples per environment
│   ├── pyspark-dataframe/ # DataFrame API
│   ├── pyspark-sql/       # Spark SQL
│   └── ...
├── spark-scala/
├── spark-java/
├── spark-r/
├── spark-sql/
└── dbx-sql/               # Databricks SQL
```

## Tech Stack & Versions

| Component | Version |
|-----------|---------|
| Apache Spark / PySpark | 3.5.x |
| Python | 3.8 – 3.12 (3.11 preferred) |
| Java | 11 (LTS) |
| Scala | 2.12 / 2.13 |
| Docker base image | `apache/spark:3.5.0-python3` |
| Documentation | MkDocs Material ≥ 9.5 |
| Testing | pytest |
| Package management | pip / conda / uv |

## General Conventions

- **No boilerplate comments.** Only comment code that needs clarification.
- Prefer `SPARK_MASTER` env var with `local[*]` fallback so every script runs locally
  without modification: `os.environ.get("SPARK_MASTER", "local[*]")`.
- Input/output paths always come from environment variables with `/tmp/...` fallbacks.
- `spark.sparkContext.setLogLevel("WARN")` in example scripts; `"ERROR"` in tests.
- Parquet is the preferred output format; CSV only when the audience is non-technical.
- Use `from pyspark.sql import functions as F` — never `import *`.
- Always call `spark.stop()` at the end of standalone scripts.
- Shell scripts: `#!/usr/bin/env bash` + `set -euo pipefail`.

## PySpark SparkSession Pattern

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("my-job")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

## Documentation Style (MkDocs Material)

- Admonitions: `!!! tip`, `!!! warning`, `!!! note`, `!!! success`, `!!! failure`.
- Tabbed variants for pip / conda / uv installation options.
- Code annotations `# (1)!` with numbered explanation lists below blocks.
- Include Python source files verbatim using `--8<-- "path/to/file.py"`.
- Mermaid diagrams for architecture overviews.
- Every page that shows code should have a runnable `Run` section.
