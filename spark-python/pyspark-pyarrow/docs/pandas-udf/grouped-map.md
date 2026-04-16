# Grouped Map UDF

A Grouped Map UDF receives a **full group** as a Pandas DataFrame and returns a
Pandas DataFrame. The output can have more, fewer, or the same number of rows.

## Signature

```python
@F.pandas_udf(result_schema, F.PandasUDFType.GROUPED_MAP)
def my_transform(pdf: pd.DataFrame) -> pd.DataFrame:
    # transform the group
    return pdf
```

## Example — Z-Score Normalisation per Group

```python
import pandas as pd
from pyspark.sql import functions as F

@F.pandas_udf(df.schema, F.PandasUDFType.GROUPED_MAP)
def normalise(pdf: pd.DataFrame) -> pd.DataFrame:    # (1)!
    pdf["score"] = (pdf["score"] - pdf["score"].mean()) / pdf["score"].std()
    return pdf

result = df.groupBy("category").apply(normalise)      # (2)!
result.show()
```

1. The group is a complete Pandas DataFrame with all columns.
2. `.apply()` is used with Grouped Map UDFs (not `.agg()`).

## When to Use

!!! success "Good fit"

    - Per-group normalisation (z-scores, min-max scaling)
    - Per-group model fitting (train one model per customer/region)
    - Any operation that needs the full group as a DataFrame

!!! failure "Not a good fit"

    - Single scalar per group → [Grouped Aggregate](grouped-aggregate.md)
    - No grouping needed → [`mapInPandas`](../arrow/map-in-pandas.md)

## Comparison with applyInPandas

| Aspect | Grouped Map UDF | `applyInPandas` |
|--------|----------------|-----------------|
| API style | Decorator-based | Function-based |
| Schema | In decorator | In `applyInPandas(fn, schema)` |
| Spark version | 2.3+ | 3.0+ |
| Recommendation | Legacy | **Preferred** |

!!! tip

    For new code, prefer `groupBy(...).applyInPandas(fn, schema)` over the
    Grouped Map UDF decorator. They do the same thing, but `applyInPandas` has
    a cleaner API.
