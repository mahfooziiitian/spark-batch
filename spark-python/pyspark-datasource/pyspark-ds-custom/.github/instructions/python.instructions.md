---
applyTo: "**/*.py"
---

# Python Style Guide

## Language & Version

- Python ≥ 3.11.
- Use modern syntax: `X | Y` unions, `match` statements, built-in generics (`list[str]`, `dict[str, int]`).
- Target PySpark 4.x APIs exclusively.

## Formatting

- Follow PEP 8.
- Maximum line length: 100 characters.
- 4-space indentation, no tabs.
- Two blank lines before top-level definitions.

## Imports

Order imports in three groups separated by a blank line, and always use
`from __future__ import annotations` as the first import:

1. Standard library (`os`, `json`, `pathlib`, `dataclasses`, …)
2. Third-party (`pyspark`, `pyarrow`, …)
3. Local / project modules (`custom_ds`, …)

Never use wildcard imports (`from module import *`).

```python
from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader

from custom_ds.session import create_spark_session
```

## Type Hints

- Add type hints to every function signature (parameters and return types).
- Use `str | None` instead of `Optional[str]`.
- Prefer `list[T]`, `dict[K, V]`, `tuple[T, ...]` over `typing.List`/`Dict`/`Tuple` where possible;
  the `pyspark.sql.datasource` classes still type-hint with `typing.Iterator`/`Tuple` in some
  signatures — match the base class signature you are overriding.

## Docstrings

- Triple double-quotes for module and class docstrings.
- Every custom `DataSource` subclass documents its `Options` and a minimal `Usage` snippet.
- Keep function docstrings concise — one line for simple helpers, multi-line for non-obvious logic.

## Naming

- `snake_case` for functions, variables, and modules.
- `PascalCase` for classes (`SimpleDataSource`, `SimpleDataSourceReader`).
- `UPPER_SNAKE_CASE` for constants (`ALL_DATA_SOURCES`).
- Data source `name()` classmethods return a short, lowercase, `snake_case` format string
  (e.g. `"simple"`, `"simple_sink"`, `"simple_stream"`).

## General

- Prefer `pathlib.Path` over `os.path`.
- Use f-strings for string formatting.
- Every partition/offset value returned to Spark (`InputPartition` subclasses, offset dicts) must
  be plain, picklable data — no open file handles, generators, or lambdas.
- Keep example scripts self-contained and runnable directly with `python <file>.py`.
- Library code in `src/custom_ds/` must be importable and testable without running as a script.
