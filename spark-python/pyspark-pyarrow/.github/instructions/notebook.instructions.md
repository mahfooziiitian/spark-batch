---
applyTo: "**/*.ipynb"
---

# Jupyter Notebook Instructions

## Cell Organisation

Structure every notebook in this order:

1. **Title cell** (markdown) — `# Notebook Title` with a one-line description.
2. **Imports cell** — all imports in a single code cell at the top.
3. **SparkSession cell** — create the session (see pattern below).
4. **Content cells** — each logical step gets its own cell with a markdown header above it.
5. **Cleanup cell** — `spark.stop()` in the final code cell.

## SparkSession Pattern

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("notebook-descriptive-name")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

## Markdown Cells

- Use markdown cells **before** each code cell to explain what the step does and why.
- Use `##` headings to create a navigable table of contents.
- Keep explanations concise — 1–3 sentences per cell.

## Code Cells

- One logical operation per cell — don't combine unrelated transformations.
- End display cells with `.show()`, `.display()`, or `.toPandas()` so output is visible.
- Avoid `print()` for DataFrames — use `.show(truncate=False)` instead.
- Never suppress cell output with `;` unless there is a deliberate reason.

## Imports

Follow the same import rules as Python scripts:

```python
import os

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
```

Never use `from pyspark.sql.functions import *`.

## Kernel & Metadata

- Use the project's Poetry virtualenv as the kernel.
- Clear all cell outputs before committing (`Cell → All Output → Clear` or
  `jupyter nbconvert --clear-output`).
- Set the kernel display name to match the virtualenv (e.g., `.venv` or the project name).

## Inline Data

Prefer small inline datasets so the notebook is self-contained:

```python
data = [("Alice", 30), ("Bob", 25), ("Carol", 35)]
df = spark.createDataFrame(data, ["name", "age"])
```

For larger datasets, load from a path with an env-var fallback:

```python
path = os.environ.get("INPUT_PATH", "data/sample.parquet")
df = spark.read.parquet(path)
```

## Cleanup

Always stop the session in the last cell:

```python
spark.stop()
```
