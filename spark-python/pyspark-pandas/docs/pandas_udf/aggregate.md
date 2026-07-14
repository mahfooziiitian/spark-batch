# Grouped Aggregate UDF

A pandas UDF that receives a `pd.Series` (all values in a group) and returns
a **scalar** — used with `groupBy().agg()`.

## Pattern

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

@pandas_udf(DoubleType())
def mean_score(v: pd.Series) -> float:
    return v.mean()

df.groupBy("group_id").agg(mean_score("score").alias("avg_score"))
```

!!! warning "Cannot mix with built-in aggregates"
    Spark does not allow pandas aggregate UDFs in the same `.agg()` call as
    built-in functions like `F.count()`. Use separate aggregations and join.

## Example

```python title="src/spp/pyspark_pandas.py"
--8<-- "src/spp/pyspark_pandas.py"
```

### Run

```bash
python src/spp/pyspark_pandas.py
```
