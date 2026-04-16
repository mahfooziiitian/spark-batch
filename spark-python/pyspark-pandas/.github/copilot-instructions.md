# GitHub Copilot Instructions — PySpark Pandas

> **Global instruction file.** Topic-specific conventions live in
> `.github/instructions/` and are auto-applied based on the file you are editing.

## Modular Instruction Files

| File | Scope (`applyTo`) | What It Covers |
| ---- | ----------------- | -------------- |
| [`pyspark-pandas.instructions.md`](instructions/pyspark-pandas.instructions.md) | `src/**/*.py` | Pandas API on Spark, pandas UDFs, Arrow optimization, UDTF patterns |
| [`testing.instructions.md`](instructions/testing.instructions.md) | `{**/test_*.py,**/*_test.py}` | Session fixture, pandas DataFrame assertions, test class layout |
| [`mkdocs.instructions.md`](instructions/mkdocs.instructions.md) | `{docs/**/*.md,mkdocs.yml}` | Material theme, page template, code annotations |

---

## Project Overview

This project is a **PySpark Pandas reference** that demonstrates the Pandas API on
Spark, pandas UDFs, Arrow optimization, and User-Defined Table Functions (UDTFs)
in PySpark 3.5.x. All examples are self-contained and runnable with `local[*]` —
no cluster required.

| Area | Module path | Key API |
| ---- | ----------- | ------- |
| Pandas on Spark DataFrame | `src/spp/pandas_on_spark/` | `pyspark.pandas`, `ps.DataFrame` |
| Pandas on Spark conversion | `src/spp/pandas_on_spark/conversion/` | `df.toPandas()`, `ps.from_pandas()` |
| Pandas on Spark options | `src/spp/pandas_on_spark/` | `ps.set_option()`, `ps.get_option()` |
| Pandas UDFs | `src/spp/pandas_udf/` | `@pandas_udf`, `F.pandas_udf` |
| Arrow optimization | `src/spp/arrow_optimization/` | `spark.sql.execution.arrow.pyspark.enabled` |
| Pandas DataFrame interop | `src/spp/dataframe/` | `spark.createDataFrame(pdf)`, `df.toPandas()` |
| Python UDTFs | `src/spp/udtf/` | `@udtf`, `udtf` decorator, table arguments |
| Notebooks | `notebooks/` | Jupyter notebooks for interactive exploration |

---

## Project Structure

```
pyspark-pandas/
├── .github/
│   ├── copilot-instructions.md          # ← you are here (global)
│   └── instructions/
│       ├── pyspark-pandas.instructions.md
│       ├── testing.instructions.md
│       └── mkdocs.instructions.md
├── src/
│   └── spp/
│       ├── pyspark_pandas.py            # Entry-point overview example
│       ├── arrow_optimization/          # Arrow-based optimization
│       │   └── arrow_optimization.py
│       ├── dataframe/                   # Pandas ↔ Spark DataFrame interop
│       │   └── pandas_dataframe.py
│       ├── pandas_on_spark/             # Pandas API on Spark
│       │   ├── pandas_on_spark_dataframe.py
│       │   ├── pandas_on_spark_ops_df.py
│       │   ├── pandas_on_spark_options.py
│       │   └── conversion/
│       │       └── dataframe_to_pandas.py
│       ├── pandas_udf/                  # pandas UDFs
│       │   └── pandas_udf.py
│       └── udtf/                        # User-Defined Table Functions
│           ├── python_udtf.py
│           ├── count_utdf.py
│           ├── data_expander.py
│           ├── udtf_sql.py
│           └── udtf_table_argument.py
├── tests/
│   └── creation/
│       └── test_pandas_creation.py
├── notebooks/                           # Jupyter notebooks
│   ├── PysparkPandas.ipynb
│   └── PysparkPandaIo.ipynb
├── docs/
│   └── python_utdf.md
├── pyproject.toml
└── uv.lock
```

---

## Tech Stack

| Component | Version |
| --------- | ------- |
| PySpark | 3.5.x |
| Python | ≥ 3.11 |
| Java | 11 (LTS) |
| pandas | ≥ 1.3.0 |
| PyArrow | ≥ 4.0.0 |
| Testing | pytest |
| Documentation | MkDocs Material ≥ 9.5 |
| Package manager | uv |

---

## Key Conventions

- **Never use `from pyspark.sql.functions import *`** — always `import functions as F`.
- **`import pyspark.pandas as ps`** — always alias Pandas API on Spark as `ps`.
- **Enable Arrow** — set `spark.sql.execution.arrow.pyspark.enabled` to `"true"` for pandas interop.
- **`SPARK_MASTER` env var** with `local[*]` fallback — every script runs locally without changes.
- **`INPUT_PATH` / `OUTPUT_PATH` env vars** with `/tmp/...` fallbacks — no hard-coded paths.
- **`spark.stop()`** at the end of every standalone script.
- **Parquet** is the preferred output format; CSV only when the audience is non-technical.

---

## SparkSession Pattern

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("descriptive-job-name")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

---

## Common Commands

```bash
# Run a specific example
SPARK_MASTER=local[*] python src/spp/pandas_udf/pandas_udf.py

# Run the full test suite
pytest tests/ -v

# Preview docs
mkdocs serve

# Build docs (strict)
mkdocs build --strict
```

---

## Things to Avoid

- **Do not** use `from pyspark.sql import *` — produces name collisions and hides intent.
- **Do not** hard-code file paths or Windows-style separators (`C:\\`, `E:\\`).
- **Do not** call `df.collect()` to count rows — use `df.count()`.
- **Do not** mix `pyspark.pandas` and `pandas` DataFrames without explicit conversion.
- **Do not** forget to enable Arrow when working with pandas interop — it dramatically improves performance.
