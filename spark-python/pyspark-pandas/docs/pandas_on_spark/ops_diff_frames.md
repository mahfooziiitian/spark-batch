# Cross-Frame Operations

By default, pandas-on-Spark disallows operations between DataFrames from
different sources. Enable `compute.ops_on_diff_frames` to allow them.

## Enable Cross-Frame Ops

```python
import pyspark.pandas as ps

ps.set_option("compute.ops_on_diff_frames", True)
```

!!! warning "Performance impact"
    Cross-frame operations trigger a shuffle join under the hood. Use
    sparingly on large DataFrames.

## Full Example

```python title="src/spp/pandas_on_spark/pandas_on_spark_ops_df.py"
--8<-- "src/spp/pandas_on_spark/pandas_on_spark_ops_df.py"
```

### Run

```bash
python src/spp/pandas_on_spark/pandas_on_spark_ops_df.py
```
