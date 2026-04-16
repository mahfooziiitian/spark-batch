# Grouped Aggregate UDF

A Grouped Aggregate UDF reduces each group to a **single scalar value** — the
Pandas UDF equivalent of a Spark aggregate function.

## Signature

```python
@F.pandas_udf("return_type", F.PandasUDFType.GROUPED_AGG)
def my_agg(values: pd.Series) -> float:
    return values.mean()
```

## Example — Weighted Mean

```python
import pandas as pd
from pyspark.sql import functions as F

@F.pandas_udf("double", F.PandasUDFType.GROUPED_AGG)
def weighted_mean(values: pd.Series) -> float:       # (1)!
    return float(values.mean())

result = df.groupBy("category").agg(weighted_mean(F.col("score")))
result.show()
```

1. The function receives **all values in the group** as a single Series and
   must return a scalar.

## Output

```
+--------+---------------------+
|category|weighted_mean(score) |
+--------+---------------------+
|       A|                 85.0|
|       B|                 72.5|
+--------+---------------------+
```

## When to Use

!!! success "Good fit"

    - Custom aggregate functions not available in Spark built-ins
    - Weighted averages, trimmed means, custom percentiles
    - Statistical functions from NumPy/SciPy

!!! failure "Not a good fit"

    - Built-in aggregations (`sum`, `avg`, `count`) — use `F.sum()` etc.
    - Transforms that return multiple rows → [Grouped Map](grouped-map.md)

## Comparison with Built-in Aggregations

| Aspect | `F.avg()` | Grouped Aggregate UDF |
|--------|-----------|----------------------|
| Implementation | JVM-native | Python via Arrow |
| Performance | Fastest | Slightly slower |
| Flexibility | Fixed set | **Custom logic** |
| Vectorized | N/A | Yes (NumPy) |
