---
applyTo: "**/*.py"
---

# Python Code Conventions

## Language Version

- Python ≥ 3.11 (see `.python-version` in each child project).
- Use modern syntax: `list[str]` over `List[str]`, `X | None` over `Optional[X]`
  (except in PySpark UDFs where `typing.Optional` is clearer).

## Style

- Follow PEP 8.
- Maximum line length: 120 characters.
- Use double quotes for strings.
- Use f-strings for interpolation.
- snake_case for functions, variables, and modules; PascalCase for classes.
- No trailing whitespace; files end with a single newline.

## Imports

Order imports in three groups separated by blank lines:

1. Standard library (`os`, `sys`, `tempfile`, `pathlib`, `typing`)
2. Third-party (`pyspark`, `delta`, `requests`)
3. Local (project modules)

```python
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
```

Never use wildcard imports:

```python
# ✅ Good
from pyspark.sql import functions as F

# ❌ Bad
from pyspark.sql.functions import *
```

## Type Hints

Add type hints to all function signatures:

```python
def load_data(path: str, format_name: str = "parquet") -> DataFrame:
    ...

def get_spark_master() -> str:
    return os.environ.get("SPARK_MASTER", "local[*]")
```

## Docstrings

Use Google-style docstrings. Keep them concise — a single imperative line
is sufficient for small helpers:

```python
def build_spark_session(app_name: str) -> SparkSession:
    """Create and configure a SparkSession for local execution."""
    ...
```

For complex functions, add Args / Returns / Raises:

```python
def read_datasource(spark: SparkSession, path: str, format_name: str) -> DataFrame:
    """Read data from a file path using the specified Spark datasource format.

    Args:
        spark: Active SparkSession instance.
        path: File or directory path to read from.
        format_name: Spark datasource format (e.g., "parquet", "csv", "json").

    Returns:
        DataFrame containing the loaded data.

    Raises:
        AnalysisException: If the path does not exist or format is unsupported.
    """
    ...
```

## Module-Level Docstrings

Every source file starts with a one-line docstring describing its purpose:

```python
"""Read JSON files into a DataFrame using the PySpark JSON datasource."""
```

## Constants

Module-level constants use UPPER_SNAKE_CASE:

```python
DEFAULT_OUTPUT_FORMAT = "parquet"
SPARK_MASTER_DEFAULT = "local[*]"
SUPPORTED_FORMATS = ["parquet", "csv", "json", "text", "orc"]
```

## Entry Points

Standalone scripts use `if __name__ == "__main__":` guard.
Test files additionally include:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```
