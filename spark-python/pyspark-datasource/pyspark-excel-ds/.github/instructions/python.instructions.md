---
applyTo: "**/*.py"
---

# Python Style Guide

## Language & Version

- Python ≥ 3.11.
- Use modern syntax: `X | Y` union types, `match` statements where appropriate.
- Target PySpark 3.5.x APIs (this project pins `pyspark>=3.5.0,<4.0.0`).

## Formatting

- Follow PEP 8; ruff handles linting/formatting (`make format`, `make lint`).
- Maximum line length: 120 characters.
- Use 4-space indentation (no tabs).
- Two blank lines before top-level definitions, one blank line between methods.

## Imports

Order imports in three groups separated by a blank line:

1. Standard library (`os`, `pathlib`, `dataclasses`, …)
2. Third-party (`pyspark`, `pandas`, `openpyxl`, `rich`, …)
3. Local / project modules (`pys_excel`, …)

Use absolute imports. Avoid wildcard imports (`from module import *`),
especially `from pyspark.sql.functions import *` — always
`from pyspark.sql import functions as F`.

```python
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from pys_excel import ExcelReader, get_spark
```

## Type Hints

- Add type hints to all function signatures (parameters and return types).
- Use `str | None` instead of `Optional[str]`.
- Use `from __future__ import annotations` for forward references and to keep
  type-only imports under `TYPE_CHECKING`.

```python
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def read_excel_sheet(spark: SparkSession, path: str) -> DataFrame: ...
```

## Docstrings

- Use triple double-quotes for module and function docstrings.
- Google-style docstring format (Args, Returns, Raises, Example).
- Document parameters only when the purpose is not obvious from the name and
  type hint — every public function in `src/pys_excel/` should still have a
  full Args/Returns docstring since these are library entry points.

## Naming

- `snake_case` for functions, variables, and modules.
- `PascalCase` for classes (`ExcelReader`, `ExcelWriter`).
- `UPPER_SNAKE_CASE` for constants (`SPARK_EXCEL_PACKAGE_SCALA_2_12`).
- Prefix private/internal helpers with a single underscore (`_pandas_to_spark`).

## General

- Prefer `pathlib.Path` over `os.path` for file operations.
- Use f-strings for string formatting (but `%s`-style args for `logger.*` calls
  — see `logging-rich.instructions.md`).
- Use context managers (`with`) for resource management.
- Keep example scripts self-contained — each file should be runnable
  independently with `python <file>.py` or `uv run python <file>.py`.
- Library code in `src/pys_excel/` must be importable and testable without
  running as a script, and must not depend on a JVM Excel package for the
  basic pandas-bridge path.
