# Series → Series UDF

The most common pandas UDF type. Receives a `pd.Series` and returns a
`pd.Series` of the same length — applied element-wise across the column.

## Pattern

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

@pandas_udf(StringType())
def upper_name(s: pd.Series) -> pd.Series:
    return s.str.upper()

df.withColumn("upper", upper_name("name"))
```

## Full Example

```python title="src/spp/pandas_udf/pandas_udf.py"
--8<-- "src/spp/pandas_udf/pandas_udf.py"
```

### Run

```bash
python src/spp/pandas_udf/pandas_udf.py
```
