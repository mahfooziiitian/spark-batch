---
applyTo: "**/*.py"
---

# Python Style Guide

## Language & Version

- Python ≥ 3.11.
- Use modern syntax: `match` statements, `type` aliases, `X | Y` union types where appropriate.
- Target PySpark 4.x APIs — use Spark Connect where applicable.

## Formatting

- Follow PEP 8.
- Maximum line length: 120 characters.
- Use 4-space indentation (no tabs).
- Two blank lines before top-level definitions, one blank line between methods.

## Imports

Order imports in three groups separated by a blank line:

1. Standard library (`os`, `json`, `pathlib`, `dataclasses`, …)
2. Third-party (`pyspark`, `pandas`, …)
3. Local / project modules (`pys_json`, …)

Use absolute imports. Avoid wildcard imports (`from module import *`).

```python
import os
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from pys_json import JsonReader, create_spark_session
```

## Type Hints

- Add type hints to all function signatures (parameters and return types).
- Use `None` return type explicitly when a function returns nothing.
- Use `str | None` instead of `Optional[str]`.
- Use `from __future__ import annotations` for forward references.

```python
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def read_json_file(spark: SparkSession, path: str) -> DataFrame:
    ...

def setup_spark(app_name: str = "json-demo") -> SparkSession:
    ...
```

## Docstrings

- Use triple double-quotes for module and function docstrings.
- Keep docstrings concise — one-liner for simple functions, multi-line for complex ones.
- Use Google-style docstring format (Args, Returns, Raises).
- Document parameters only when the purpose is not obvious from the name and type hint.

## Naming

- `snake_case` for functions, variables, and modules.
- `PascalCase` for classes.
- `UPPER_SNAKE_CASE` for constants.
- Prefix private/internal helpers with a single underscore (`_create_temp_file`).

## General

- Prefer `pathlib.Path` over `os.path` for file operations.
- Use f-strings for string formatting.
- Use context managers (`with`) for resource management.
- Keep example scripts self-contained — each file should be runnable independently with `python <file>.py`.
- Library code in `src/pys_json/` must be importable and testable without running as a script.
