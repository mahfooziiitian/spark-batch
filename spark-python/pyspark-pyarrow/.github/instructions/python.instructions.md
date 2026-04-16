---
applyTo: "**/*.py"
---

# Python Code Instructions

## Style & Formatting

- Target **Python 3.11** (minimum 3.8 for compatibility).
- Use **type hints** on all function signatures:

```python
def process_rows(df: DataFrame, column: str) -> DataFrame:
    ...
```

- Prefer `pathlib.Path` over `os.path` for file operations outside Spark I/O.
- Use f-strings for string formatting — never `%` or `.format()`.

## Imports

Order imports in three groups separated by blank lines:

```python
import os                           # 1. stdlib
from pathlib import Path

import pandas as pd                 # 2. third-party
from pyspark.sql import SparkSession

from mypackage.utils import helper  # 3. local / project
```

Never use wildcard imports (`from module import *`).

## Docstrings

Use a concise single-line or Google-style multi-line docstring:

```python
def remove_extra_spaces(df: DataFrame, column_name: str) -> DataFrame:
    """Collapse consecutive whitespace into a single space."""
    ...

def compute_revenue(df: DataFrame, region: str) -> DataFrame:
    """Compute total revenue by month for a region.

    Args:
        df: Input DataFrame with 'region', 'month', and 'revenue' columns.
        region: Region code to filter on.

    Returns:
        DataFrame with 'month' and 'total_revenue' columns.
    """
    ...
```

## Constants & Configuration

- Constants at module level in `UPPER_SNAKE_CASE`.
- Environment variables with safe fallbacks:

```python
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/output")
```

## Error Handling

- Catch specific exceptions — never bare `except:`.
- Re-raise or log with context:

```python
try:
    df = spark.read.parquet(path)
except AnalysisException as exc:
    raise FileNotFoundError(f"Parquet not found at {path}") from exc
```

## Module Entry Points

Standalone scripts must guard execution:

```python
if __name__ == "__main__":
    main()
```

## Comments

Only comment code that needs clarification. No boilerplate comments like
`# import libraries` or `# create spark session`.
