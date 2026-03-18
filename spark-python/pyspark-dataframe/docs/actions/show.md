# Show

Print rows to stdout in a formatted table. `show()` is an action — it triggers
computation and collects rows to the driver — but it does **not** return a value.

## API Reference

| Signature | Default | Description |
|-----------|---------|-------------|
| `show()` | 20 rows, truncate 20 chars | Default tabular output |
| `show(n)` | truncate 20 chars | First *n* rows |
| `show(truncate=False)` | 20 rows | Full column values, no truncation |
| `show(truncate=N)` | 20 rows | Truncate each column at *N* characters |
| `show(vertical=True)` | 20 rows | One column-value pair per line |
| `show(n, truncate, vertical)` | — | All options combined |

## Examples

### Default show

```python
df.show()                                    # (1)!
```
1. Displays the first 20 rows with columns truncated at 20 characters.

### Control row count

```python
df.show(3)                                   # first 3 rows
df.show(100)                                 # shows all if fewer than 100
```

### Truncation control

```python
df.show()                                    # truncate at 20 chars (default)
df.show(truncate=False)                      # full column values
df.show(truncate=40)                         # truncate at 40 chars
```

### Vertical layout — wide schemas

```python
df.show(3, vertical=True)                    # (1)!
df.show(2, truncate=False, vertical=True)
```
1. Vertical mode prints one `(column: value)` pair per line — ideal for
   DataFrames with many columns that don't fit in a horizontal table.

### show() after transformations

```python
from pyspark.sql import functions as F

(
    df.filter(F.col("status") == "active")
    .select(
        "order_id",
        "product",
        F.round(F.col("quantity") * F.col("unit_price"), 2).alias("line_total"),
    )
    .orderBy(F.desc("line_total"))
    .show(truncate=False)                    # (1)!
)
```
1. Calling `show()` at the end of a chain is a common debugging pattern —
   it does not interrupt further chaining if needed.

### NULLs in show output

```python
from pyspark.sql import functions as F

df.filter(F.col("customer_id").isNull()).show(truncate=False)
# NULL values appear as the literal string "null" in the output
```

### Run

```bash
cd spark-python/pyspark-dataframe
python src/data_frame/actions/show/action_show.py
```

!!! tip "Use vertical=True for wide DataFrames"
    When a DataFrame has many columns, the default horizontal table wraps
    and becomes unreadable. Switch to `vertical=True` for a clean layout.

!!! warning "show() collects rows to the driver"
    Although `show()` only prints, it still transfers `n` rows to the driver.
    On a DataFrame with very wide rows (large strings, arrays), even a small
    `n` can use significant memory.

!!! note "show() returns None"
    `show()` prints to stdout and returns `None`. To capture the formatted
    string, use `df._jdf.showString(n, truncate, vertical)` (internal API).

## Full Source

```python title="src/data_frame/actions/show/action_show.py"
--8<-- "src/data_frame/actions/show/action_show.py"
```
