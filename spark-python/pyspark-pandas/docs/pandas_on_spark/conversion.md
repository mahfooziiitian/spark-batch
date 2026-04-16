# DataFrame Conversion

Convert between **pandas**, **Spark**, and **pandas-on-Spark** DataFrames.

## Conversion Reference

| From | To | Method |
|------|------|--------|
| pandas | pandas-on-Spark | `ps.from_pandas(pdf)` |
| pandas-on-Spark | pandas | `psdf.to_pandas()` |
| Spark | pandas-on-Spark | `sdf.pandas_api()` |
| pandas-on-Spark | Spark | `psdf.to_spark()` |
| Spark | pandas | `sdf.toPandas()` |
| pandas | Spark | `spark.createDataFrame(pdf)` |

!!! tip "Enable Arrow"
    Always set `spark.sql.execution.arrow.pyspark.enabled` to `true` for
    fast Spark ↔ pandas transfers.

## Full Example

```python title="src/spp/pandas_on_spark/conversion/dataframe_to_pandas.py"
--8<-- "src/spp/pandas_on_spark/conversion/dataframe_to_pandas.py"
```

### Run

```bash
python src/spp/pandas_on_spark/conversion/dataframe_to_pandas.py
```
