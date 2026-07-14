---
applyTo: "**/*.py"
---

# Python Style Instructions

## General

- Follow **PEP 8**. Max line length: **100 characters**.
- Use `snake_case` for functions, variables, and module names.
- Use `PascalCase` for class names.
- Use `UPPER_SNAKE_CASE` for module-level constants.
- Never use `from module import *`.

## Type Hints

All function signatures must have type hints:

```python
def count_region(region: str, spark: SparkSession) -> tuple[str, int]:
    ...
```

Use `|` union syntax (Python 3.10+) instead of `Optional[X]`:
```python
def load_path(path: str | None = None) -> DataFrame:
    ...
```

## Imports

Order: stdlib → third-party → local. Separate each group with a blank line:

```python
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing.pool import ThreadPool
from threading import Lock

import pytest

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
```

## Docstrings

Use **Google style** for public modules and functions:

```python
def benchmark(label: str, df: DataFrame, pool_size: int) -> list:
    """Run a duplicate-count benchmark with a given pool size.

    Args:
        label: Display name for this benchmark run.
        df: Source DataFrame to analyse.
        pool_size: Number of threads in the ThreadPool.

    Returns:
        List of (column_name, duplicate_count) tuples.
    """
```

Module docstrings should briefly describe:
1. What pattern the file demonstrates.
2. Key design decisions or trade-offs.
3. Environment variables that control behaviour.

## Constants and Environment Variables

Declare all env-var reads at module top level, never inline:

```python
JDBC_URL    = os.environ.get("JDBC_URL", "")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/output")
```

## Error Handling

- Use specific exception types, not bare `except`.
- In thread workers, catch exceptions, log them, and re-raise or store for the caller:

```python
def worker(item: str) -> str:
    try:
        return process(item)
    except Exception as exc:
        raise RuntimeError(f"Failed to process {item!r}") from exc
```

## Entry Points

Every standalone script must include:

```python
if __name__ == "__main__":
    main()
```

Every test file must include:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```
