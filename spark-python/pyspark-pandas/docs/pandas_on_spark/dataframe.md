# Pandas-on-Spark DataFrame

Create and manipulate DataFrames using the familiar pandas API, backed by
Spark's distributed engine.

## Creation

### From a dict

```python
import pyspark.pandas as ps

psdf = ps.DataFrame({
    "name":  ["Alice", "Bob", "Carol", "Dave", "Eve"],
    "age":   [30, 25, 35, 28, 32],
    "score": [85.5, 92.0, 78.0, 88.5, 95.0],
})
```

### From `ps.range()`

```python
psdf = ps.range(10)  # single column 'id' with values 0..9
```

### From a pandas DataFrame

```python
import pandas as pd

pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
psdf = ps.from_pandas(pdf)
```

## Common Operations

| Operation | Code |
|-----------|------|
| Head | `psdf.head(5)` |
| Describe | `psdf.describe()` |
| Sort | `psdf.sort_values("score", ascending=False)` |
| Column types | `psdf.dtypes` |
| Column selection | `psdf[["name", "score"]]` |
| Boolean filter | `psdf[psdf["score"] > 85]` |
| Value counts | `psdf["city"].value_counts()` |

## Full Example

```python title="src/spp/pandas_on_spark/pandas_on_spark_dataframe.py"
--8<-- "src/spp/pandas_on_spark/pandas_on_spark_dataframe.py"
```

### Run

```bash
python src/spp/pandas_on_spark/pandas_on_spark_dataframe.py
```
