# Arithmetic Functions

Column-level arithmetic and logic functions. Each operates on PySpark `Column`
objects and returns a `Column`, suitable for use with `df.withColumn()`.

## Source

```python title="src/data_frame/functions/functions.py"
--8<-- "src/data_frame/functions/functions.py"
```

## Functions

### divide_by_three

Simple division — useful as a minimal example of column arithmetic.

```python
from data_frame.functions.functions import divide_by_three

df = df.withColumn("result", divide_by_three(F.col("num")))
```

### null_safe_divide

Divides two columns, returning `null` instead of error when the denominator
is zero or null.

```python
from data_frame.functions.functions import null_safe_divide

df = df.withColumn("ratio", null_safe_divide(F.col("revenue"), F.col("cost")))
```

| numerator | denominator | result |
| --- | --- | --- |
| `10.0` | `2.0` | `5.0` |
| `10.0` | `0.0` | `null` |
| `10.0` | `null` | `null` |
| `null` | `5.0` | `null` |

!!! tip "Use for safe metrics"
    `null_safe_divide` is the building block for `percentage` and any ratio
    calculation where division by zero is possible.

### percentage

Calculates the percentage of `part` relative to `total`, rounded to the
specified number of decimal places.

```python
from data_frame.functions.functions import percentage

df = df.withColumn("pct", percentage(F.col("sales"), F.col("total_sales")))
# Custom precision
df = df.withColumn("pct_1dp", percentage(F.col("sales"), F.col("total_sales"), decimals=1))
```

| part | total | result (decimals=2) |
| --- | --- | --- |
| `25.0` | `100.0` | `25.0` |
| `1.0` | `3.0` | `33.33` |
| `10.0` | `0.0` | `null` |

### clamp

Restricts column values to a `[lower, upper]` range.

```python
from data_frame.functions.functions import clamp

df = df.withColumn("score_clamped", clamp(F.col("score"), 0.0, 100.0))
```

| value | lower | upper | result |
| --- | --- | --- | --- |
| `5.0` | `0.0` | `100.0` | `5.0` |
| `-5.0` | `0.0` | `100.0` | `0.0` |
| `150.0` | `0.0` | `100.0` | `100.0` |

!!! warning "Invalid bounds"
    Raises `ValueError` if `lower > upper`.

## Run Tests

```bash
uv run pytest tests/functions/ -v
```
