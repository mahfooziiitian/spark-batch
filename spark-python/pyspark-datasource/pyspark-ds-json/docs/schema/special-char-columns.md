# Special Characters in Column Names

Handling JSON fields with dots, spaces, hyphens, slashes, or SQL reserved words.

## The Problem

```json
{"user.id": 1, "first name": "Mahfooz", "last-name": "Alam", "select": "admin"}
```

!!! failure "Why This Is Hard"
    - Dots `.` conflict with nested field access syntax
    - Spaces break column references
    - Hyphens `-` are interpreted as minus
    - Reserved words (`select`, `from`, `table`) conflict with SQL
    - Slashes `/` break path-like references

## Backtick Escaping

Wrap special column names in backticks for all references:

```python
from pyspark.sql import functions as F

df.select(
    F.col("`user.id`"),
    F.col("`first name`"),
    F.col("`last-name`"),
    F.col("`select`"),
)
```

Works in SQL too:

```sql
SELECT `user.id`, `first name`, `select`
FROM special_table
WHERE `user.id` > 1
```

## Renaming Strategies

### Individual rename

```python
df_clean = (
    df.withColumnRenamed("user.id", "user_id")
    .withColumnRenamed("first name", "first_name")
    .withColumnRenamed("select", "select_value")
)
```

### Select + alias (most explicit)

```python
df_clean = df.select(
    F.col("`user.id`").alias("user_id"),
    F.col("`first name`").alias("first_name"),
    F.col("`select`").alias("role"),
)
```

### Bulk rename with `toDF()`

```python
df_clean = df.toDF("user_id", "first_name", "last_name", "role")
```

!!! warning
    `toDF()` renames by **position** — ensure the order matches `df.columns` exactly.

## Automatic Normalization

```python
import re

def normalize_column_name(name: str) -> str:
    """Convert any column name to safe snake_case."""
    cleaned = re.sub(r"[.\s\-/]+", "_", name)
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned.strip("_").lower()

df_clean = df.toDF(*[normalize_column_name(c) for c in df.columns])
```

## Nested Structs with Special Characters

Chain backticks for nested access:

```python
df.select(
    F.col("`user info`.`first.name`").alias("first_name"),
    F.col("`meta data`.`created at`").alias("created_at"),
)
```

## Full Demo

```python title="examples/06_schema/20_special_char_columns.py"
--8<-- "examples/06_schema/20_special_char_columns.py"
```

## Run

```bash
python examples/06_schema/20_special_char_columns.py
```

## Quick Reference

| Character | Example | Access Pattern |
|-----------|---------|----------------|
| Dot `.` | `user.id` | `` col("`user.id`") `` |
| Space | `first name` | `` col("`first name`") `` |
| Hyphen `-` | `last-name` | `` col("`last-name`") `` |
| Slash `/` | `data/path` | `` col("`data/path`") `` |
| Reserved word | `select` | `` col("`select`") `` |
| Nested | `parent.child` | `` col("`parent`.`child`") `` |

!!! success "Best Practice"
    Normalize column names **immediately after bronze ingestion**.
    This eliminates backtick escaping in all downstream queries,
    joins, and aggregations.
