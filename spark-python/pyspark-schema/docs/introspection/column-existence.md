# Column Existence

## Top-Level Columns

The simplest existence check uses `fieldNames()`:

```python
"name" in df.schema.fieldNames()    # True
"missing" in df.schema.fieldNames() # False
```

For a strict match (name + type + nullable), use the `in` operator on
`schema.fields`:

```python
from pyspark.sql.types import StructField, StringType

target = StructField("name", StringType(), True)
target in df.schema.fields   # True only if all three attributes match
```

## Nested Columns — `has_column`

Top-level checks don't work for dot-notation paths like `"foo.bar.baz"`.
Use `AnalysisException` to probe any path:

```python
from pyspark.sql.utils import AnalysisException

def has_column(df: DataFrame, col: str) -> bool:
    try:
        df[col]
        return True
    except AnalysisException:
        return False
```

```python
has_column(df, "foo")            # True
has_column(df, "foo.bar")        # True
has_column(df, "foo.bar.foobar") # True
has_column(df, "foo.bar.foobaz") # False
```

!!! warning
    Use `has_column` only for existence checks. Do not catch `AnalysisException`
    broadly for general error handling.

## Traversing Nested Fields Programmatically

```python
# Nested field names of a specific struct column
metrics_fields = df.schema["metrics"].dataType.fieldNames()
"age" in metrics_fields   # True
"bmi" in metrics_fields   # False
```

## Code

```python title="src/column/has_column.py"
--8<-- "src/column/has_column.py"
```

```python title="src/column/column_existence.py"
--8<-- "src/column/column_existence.py"
```

## Run

```bash
SPARK_MASTER=local[*] python src/column/has_column.py
SPARK_MASTER=local[*] python src/column/column_existence.py
```

## Key Points

- `fieldNames()` only returns **top-level** column names.
- `has_column` handles dot-notation paths at any nesting depth.
- Prefer `has_column` before accessing an optional column that may not exist in all data sources.
