---
applyTo: "**/*.py"
---

# Python Code Conventions

## Language Version

- Python ≥ 3.11 (see `.python-version`).
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

1. Standard library (`os`, `tempfile`, `gzip`, `bz2`, `typing`)
2. Third-party (`pyspark`)
3. Local (project modules)

```python
import os
import tempfile
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType
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
def parse_line(line: str) -> dict[str, str | None]:
    ...

def count_words(text: str) -> int:
    ...
```

## Docstrings

Use Google-style docstrings. Keep them concise — a single imperative line
is sufficient for small helpers:

```python
def parse_line(line: str) -> dict[str, str | None]:
    """Parse a pipe-delimited text line into a field dictionary."""
    ...
```

For complex functions, add Args / Returns / Raises:

```python
def extract_fields(line: str, delimiter: str = "|") -> list[str]:
    """Split a text line into fields using the given delimiter.

    Args:
        line: Raw text line to parse.
        delimiter: Field separator character.

    Returns:
        List of field values as strings.

    Raises:
        ValueError: If line is empty.
    """
    ...
```

## Module-Level Docstrings

Every source file starts with a one-line docstring describing its purpose:

```python
"""Read a text file into a DataFrame — one row per line, single 'value' column."""
```

## Constants

Module-level constants use UPPER_SNAKE_CASE:

```python
DEFAULT_ENCODING = "UTF-8"
SUPPORTED_COMPRESSIONS = ["gzip", "bzip2", "deflate", "none"]
```

## Entry Points

Standalone scripts use `if __name__ == "__main__":` guard.
Test files additionally include:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```
