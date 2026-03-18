---
applyTo: "spark-python/pyspark-test/pyspark-chispa/src/**/*.py"
---

# PySpark Source Code Instructions

## Imports

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
| `functions/`     | Column math / logic functions      | Yes              |
| `transformation/` | DataFrame-level transformations   | Yes              |
| `helper/`        | Pure Python utilities              | **No**           |
| `schema/`        | Schema utilities                   | Yes              |

Keep pure Python helpers (string manipulation, formatting, etc.) in `helper/`
so they can be unit-tested without a SparkSession.

## Function Signatures

- Column functions receive and return PySpark `Column` objects:
  ```python
  def remove_non_word_characters(col):
      return F.regexp_replace(col, "[^\\w\\s]+", "")
  ```

- DataFrame functions receive and return `DataFrame` objects:
  ```python
  def sort_columns(df, sort_order):
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

### Column function

```python
def remove_non_word_characters(col: Column) -> Column:
    """Remove all non-word characters from a string column.

    Strips everything except word characters (``\\w``) and whitespace (``\\s``).

    Args:
        col: Input string column.

    Returns:
        Column with non-word characters removed.

    Example:
        >>> df.withColumn("clean", remove_non_word_characters(F.col("name")))
    """
    return F.regexp_replace(col, "[^\\w\\s]+", "")
```

### DataFrame function

```python
def sort_columns(df: DataFrame, sort_order: str) -> DataFrame:
    """Reorder DataFrame columns alphabetically.

    Args:
        df: Input DataFrame.
        sort_order: ``"asc"`` for ascending or ``"desc"`` for descending.

    Returns:
        DataFrame with columns sorted in the specified order.

    Raises:
        ValueError: If ``sort_order`` is not ``"asc"`` or ``"desc"``.
    """
```

### Pure Python helper

```python
def dots_to_underscores(s: str) -> str:
    """Replace all dots with underscores in a string.

    Args:
        s: Input string.

    Returns:
        String with dots replaced by underscores.
    """
    return s.replace(".", "_")
```

### Class

```python
class ColumnCleaner:
    """Collection of column-level cleaning transformations.

    Attributes:
        pattern: Compiled regex pattern used for cleaning.
    """
```

### Key rules

- **Always include**: one-line summary, `Args`, `Returns`.
- **Include when relevant**: `Raises`, `Example`, `Note`, `Attributes`.
- First line is an imperative summary ending with a period.
- Blank line between summary and `Args` section.
- Use double backticks for inline code references in docstrings.
- Keep `Example` blocks valid for quick manual verification.

## Error Handling

Raise `ValueError` with descriptive messages for invalid arguments:

```python
raise ValueError(
    f"['asc', 'desc'] are the only valid sort orders and you entered '{sort_order}'"
)
```

## Code Style

- Line length: 120 characters (configured in `[tool.ruff]`).
- Linting rules: E, F, W, I (isort), UP (pyupgrade), B (bugbear), SIM, RUF.
- Run `uv run task lint` before committing.
- Run `uv run task format` to auto-format.
