# Iterator UDF

Receives an `Iterator[pd.Series]` and yields an `Iterator[pd.Series]` — useful
for **stateful** or **batched** transforms where you need to process data in
chunks or maintain state across batches.

## Pattern

```python
from typing import Iterator
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

@pandas_udf(DoubleType())
def normalize(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for batch in batch_iter:
        yield (batch - batch.mean()) / batch.std()
```

## When to Use

!!! success "Good fit"
    - Expensive one-time setup (load ML model once, score many batches)
    - Stateful transforms (running averages, counters)
    - Memory-efficient processing of large partitions

!!! note
    The iterator UDF sees one partition at a time. State does **not** carry
    across partitions.
