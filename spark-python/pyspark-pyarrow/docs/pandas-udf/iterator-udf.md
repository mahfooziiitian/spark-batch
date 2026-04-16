# Iterator UDF

An Iterator UDF processes data in **streaming batches**, allowing expensive
one-time initialisation (model loading, database connections) to happen once
per worker instead of once per batch.

## Signature

```python
from typing import Iterator

@F.pandas_udf("return_type")
def my_udf(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
    model = load_model()           # expensive — runs once per worker
    for batch in batches:
        yield model.predict(batch) # cheap — runs per batch
```

## Example — Cumulative Sum

```python
from typing import Iterator

@F.pandas_udf("double")
def cumulative_sum(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
    running = 0.0
    for batch in batches:                    # (1)!
        running += batch.sum()
        yield pd.Series([running] * len(batch))

df.select(cumulative_sum(F.col("x"))).show()
```

1. Each batch is a `pd.Series`. State persists across batches within the same
   partition.

## When to Use

!!! success "Good fit"

    - ML model inference (load model once, score batches)
    - Database lookups (open connection once)
    - Any UDF with expensive setup

!!! failure "Not a good fit"

    - Simple element-wise math → [Series → Series](series-to-series.md) is simpler
    - Group-level operations → [Grouped Map](grouped-map.md)

## Comparison with Series → Series

| Aspect | Series → Series | Iterator |
|--------|----------------|----------|
| Init cost | Per batch | **Per partition** (once) |
| Stateful | No | Yes (across batches) |
| Signature | `Series → Series` | `Iterator[Series] → Iterator[Series]` |
| Complexity | Simpler | Slightly more complex |
