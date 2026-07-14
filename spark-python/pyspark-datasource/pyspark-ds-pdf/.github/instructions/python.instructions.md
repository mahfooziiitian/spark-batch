---
applyTo: "**/*.py"
---

# Python Code Conventions

## Language Version

- Python ≥ 3.11 (see `.python-version`).
- Use modern syntax: `list[str]` over `List[str]`, `X | None` over `Optional[X]`.

## Style

- Follow PEP 8.
- Maximum line length: 120 characters.
- Use double quotes for strings.
- Use f-strings for interpolation.
- `snake_case` for functions, variables, and modules; `PascalCase` for classes.
- No trailing whitespace; files end with a single newline.

## Imports

Order imports in three groups separated by blank lines:

1. Standard library (`os`, `pathlib`, `re`, `typing`)
2. Third-party (`pyspark`, `fpdf`)
3. Local (project modules)

```python
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

from pdf.pdf_reader import create_spark_session, read_pdf
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
def read_pdf(spark: SparkSession, path: str, *, image_type: str = "RGB") -> DataFrame:
    ...
```

## Docstrings

Use Google-style docstrings. A single imperative line is sufficient for small helpers:

```python
def read_pdf(spark: SparkSession, path: str) -> DataFrame:
    """Load one or more PDF files into a Spark DataFrame."""
```

For complex functions, add Args / Returns:

```python
def read_pdf(spark: SparkSession, path: str, *, resolution: str = "200") -> DataFrame:
    """Load one or more PDF files into a Spark DataFrame.

    Args:
        spark: Active SparkSession configured with the spark-pdf package.
        path: Glob-friendly path to PDF file(s), e.g. "/data/docs/*.pdf".
        resolution: DPI for page rendering. Lower values are faster.

    Returns:
        DataFrame with columns: path, page_number, text, image, document, partition_number.
    """
```

## Module-Level Docstrings

Every source file starts with a one-line module docstring:

```python
"""Read PDF files into Spark DataFrames using the spark-pdf data source."""
```

## Constants

Module-level constants use `UPPER_SNAKE_CASE`:

```python
_SPARK_PDF_PACKAGE = "com.stabrise:spark-pdf-spark35_2.12:0.1.16"
_DEFAULTS = {"imageType": "RGB", "resolution": "200"}
```

## Entry Points

Standalone scripts use an `if __name__ == "__main__":` guard.
Test files additionally include:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```
