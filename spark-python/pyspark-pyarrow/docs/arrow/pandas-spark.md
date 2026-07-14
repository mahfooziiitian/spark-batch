# Pandas ↔ Spark Conversions

Arrow-optimized conversions between Pandas and Spark DataFrames eliminate
row-by-row serialization, using columnar batches instead.

## createDataFrame — Pandas → Spark

```python
import pandas as pd
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
         .getOrCreate())

pdf = pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": [4.0, 5.0, 6.0]})
df = spark.createDataFrame(pdf)   # (1)!
df.show()
```

1. With Arrow enabled, this uses columnar batches instead of row-by-row pickling.

## toPandas — Spark → Pandas

```python
result_pdf = df.select("*").toPandas()  # (1)!
print(result_pdf.describe())
```

1. Arrow transfers entire columns at once — much faster for wide DataFrames.

!!! warning "Memory"

    `toPandas()` collects **all data** to the driver. Only use on results that fit
    in driver memory.

## Performance Comparison

| Method | 100K rows × 10 cols | Arrow speedup |
|--------|---------------------|---------------|
| `createDataFrame` (no Arrow) | ~8 s | — |
| `createDataFrame` (Arrow) | ~0.3 s | **~25×** |
| `toPandas` (no Arrow) | ~12 s | — |
| `toPandas` (Arrow) | ~0.5 s | **~24×** |

!!! note

    Actual speedups depend on data types, row count, and JVM configuration.
    Arrow benefits increase with larger datasets.

## Type Mapping

| Pandas dtype | Spark type | Notes |
|-------------|-----------|-------|
| `int64` | `LongType` | |
| `float64` | `DoubleType` | |
| `object` (str) | `StringType` | |
| `bool` | `BooleanType` | |
| `datetime64[ns]` | `TimestampType` | timezone-aware requires `TimestampNTZType` |
| `category` | `StringType` | converted to string |

## Run

```bash
python src/psa/pyspark_pyarrow.py
```

## Full Example

```python title="src/psa/pyspark_pyarrow.py"
--8<-- "src/psa/pyspark_pyarrow.py"
```
