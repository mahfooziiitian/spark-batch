---
applyTo: "spark-python/pyspark-test/pyspark-chispa/src/**/*.py"
---

# PySpark Source Code Instructions — pyspark-chispa

## Imports

```python
from pyspark.sql import SparkSession
from pyspark.sql import Column, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
```

Never use `from pyspark.sql.functions import *`.

## Module Organisation

Source code lives under `src/data_frame/` with subpackages by domain:

| Package          | Purpose                            | Spark dependency |
| ---------------- | ---------------------------------- | ---------------- |
| `columns/`       | Column-level transformations       | Yes              |
| `equality/`      | DataFrame comparison utilities     | Yes              |
| `functions/`     | Arithmetic column functions        | Yes              |
| `transformation/` | DataFrame-level transformations   | Yes              |
| `helper/`        | Pure Python utilities              | **No**           |
| `schema/`        | Schema inspection and conversion   | Yes              |

Keep pure Python helpers (string manipulation, formatting, etc.) in `helper/`
so they can be unit-tested without a SparkSession.

## Function Signatures

- Column functions receive and return PySpark `Column` objects:
  ```python
  def remove_non_word_characters(col: Column) -> Column:
      return F.regexp_replace(col, "[^\\w\\s]+", "")
  ```

- DataFrame functions receive and return `DataFrame` objects:
  ```python
  def sort_columns(df: DataFrame, sort_order: str) -> DataFrame:
      ...
      return df.select(*sorted_col_names)
  ```

- Pure helpers operate on plain Python types:
  ```python
  def dots_to_underscores(s: str) -> str:
      return s.replace(".", "_")
  ```

## Type Hints

Add type hints to all function signatures. Use PySpark types:

```python
from pyspark.sql import Column, DataFrame


def remove_non_word_characters(col: Column) -> Column: ...
def sort_columns(df: DataFrame, sort_order: str) -> DataFrame: ...
```

## Docstrings

Use **Google-style docstrings** on all public functions, classes, and modules.

### Key rules

- **Always include**: one-line summary, `Args`, `Returns`.
- **Include when relevant**: `Raises`, `Example`, `Note`, `Attributes`.
- First line is an imperative summary ending with a period.
- Blank line between summary and `Args` section.
- Use double backticks for inline code references in docstrings.

## Error Handling

Raise `ValueError` with descriptive messages for invalid arguments:

```python
raise ValueError(f"['asc', 'desc'] are the only valid sort orders and you entered '{sort_order}'")
```

## Code Style

- Line length: 120 characters (configured in `[tool.ruff]`).
- Linting rules: E, F, W, I (isort), UP (pyupgrade), B (bugbear), SIM, RUF.
- Run `uv run task lint` before committing.
- Run `uv run task format` to auto-format.
