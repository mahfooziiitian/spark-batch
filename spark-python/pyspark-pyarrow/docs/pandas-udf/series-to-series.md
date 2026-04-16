# Series → Series UDF

The most common Pandas UDF type. Receives one or more `pd.Series` columns and
returns a `pd.Series` of the same length.

## Signature

```python
@F.pandas_udf("return_type")
def my_udf(col1: pd.Series, col2: pd.Series) -> pd.Series:
    return col1 * col2
```

## Example — Multiply Two Columns

```python
import pandas as pd
from pyspark.sql import functions as F

@F.pandas_udf("double")
def multiply(a: pd.Series, b: pd.Series) -> pd.Series:  # (1)!
    return a * b

df.select(multiply(F.col("x"), F.col("x"))).show()
```

1. Each batch is a Pandas Series — NumPy vectorization applies automatically.

## Output

```
+------------------+
|multiply(x, x)    |
+------------------+
|               1.0|
|               4.0|
|               9.0|
+------------------+
```

## When to Use

!!! success "Good fit"

    - Element-wise arithmetic or string operations
    - NumPy/SciPy functions on columns
    - Any transform that maps N rows → N rows

!!! failure "Not a good fit"

    - Aggregations → use [Grouped Aggregate](grouped-aggregate.md)
    - Operations needing full group context → use [Grouped Map](grouped-map.md)
    - Expensive one-time setup (model loading) → use [Iterator UDF](iterator-udf.md)

## Spark vs Pandas UDF

| Aspect | Regular UDF | Series → Series UDF |
|--------|-----------|---------------------|
| Data format | One row at a time | Columnar batch |
| Transfer | Pickle | Arrow |
| Speed | Baseline | **~3–100× faster** |
| Vectorized | No | Yes (NumPy) |
