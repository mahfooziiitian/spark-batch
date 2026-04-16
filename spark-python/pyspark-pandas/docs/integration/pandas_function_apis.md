# Pandas Function APIs

PySpark 3.0+ introduces three **pandas function APIs** that let you apply
Python functions to distributed DataFrames using `pd.DataFrame` as the
interface. Unlike Pandas UDFs (which work on `pd.Series` columns), these APIs
operate on **entire DataFrames** — making them ideal for multi-column logic.

## Overview

| API | Signature | Use Case |
|-----|-----------|----------|
| `mapInPandas` | `Iterator[pd.DataFrame] → Iterator[pd.DataFrame]` | General row-wise transforms |
| `applyInPandas` | `pd.DataFrame → pd.DataFrame` (per group) | Group-wise operations |
| `cogroup.applyInPandas` | `(pd.DataFrame, pd.DataFrame) → pd.DataFrame` | Joining grouped data |

## mapInPandas

Apply a function to each partition as a batch of rows:

```python
from typing import Iterator
import pandas as pd

def add_features(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for pdf in iterator:
        pdf["name_upper"] = pdf["name"].str.upper()
        pdf["age_bucket"] = pd.cut(pdf["age"], bins=[0, 30, 60, 100],
                                   labels=["young", "middle", "senior"])
        yield pdf

result = df.mapInPandas(add_features, schema=output_schema)
```

!!! tip "Memory efficient"
    The iterator pattern processes one batch at a time — ideal for large
    partitions that don't fit in memory all at once.

See [mapInPandas](../pandas_udf/map_in_pandas.md) for the full guide.

---

## applyInPandas

Split-apply-combine per group:

```python
def normalize(pdf: pd.DataFrame) -> pd.DataFrame:
    for col in ["feature_a", "feature_b"]:
        mean, std = pdf[col].mean(), pdf[col].std() or 1.0
        pdf[col] = (pdf[col] - mean) / std
    return pdf

result = df.groupBy("category").applyInPandas(normalize, schema=output_schema)
```

!!! warning "Memory"
    All data for a group is loaded into a single executor's memory.
    Ensure groups aren't too large.

See [Grouped Map](../pandas_udf/grouped_map.md) for the full guide.

---

## cogroup.applyInPandas

Join two grouped DataFrames per key:

```python
def merge_data(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(left, right, on="id", how="inner")

result = (
    df1.groupBy("id")
    .cogroup(df2.groupBy("id"))
    .applyInPandas(merge_data, schema=output_schema)
)
```

See [Cogrouped Map](../pandas_udf/cogroup.md) for the full guide.

---

## Performance

### Arrow Batch Size

Control the batch size for `mapInPandas` and pandas UDFs:

```python
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "5000")  # (1)!
```
1. Smaller batches use less memory; larger batches improve throughput.

### Shuffle Partitions

For `applyInPandas` and `cogroup`, reduce shuffle partitions for small data:

```python
spark.conf.set("spark.sql.shuffle.partitions", "4")  # (1)!
```
1. Default 200 is wasteful for small datasets or few groups.

## When to Use Which

| Method | Best For |
|--------|----------|
| `mapInPandas` | Row-wise transforms needing multiple columns |
| `applyInPandas` | Per-group statistics, normalization, time-series |
| `cogroup.applyInPandas` | Complex joins, as-of joins, cross-dataset enrichment |
| Pandas UDF (`pd.Series`) | Single-column element-wise transforms |

## Run

```bash
python src/spp/integration/pandas_function_apis.py
```

## Full Example

```python title="src/spp/integration/pandas_function_apis.py"
--8<-- "src/spp/integration/pandas_function_apis.py"
```
