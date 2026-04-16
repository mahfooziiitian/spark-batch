# PySpark + PyArrow

Apache Arrow is an in-memory columnar data format used by Spark to efficiently transfer
data between the JVM and Python processes. This module demonstrates Arrow-optimized
PySpark patterns including conversions, Pandas UDFs, and UDTFs.

## Examples

| File | What it demonstrates |
|------|---------------------|
| `src/psa/pyspark_pyarrow.py` | Arrow-enabled `createDataFrame`, `toPandas`, `mapInPandas`, `applyInPandas` |
| `src/psa/pandas_udf_spark.py` | Series→Series, Iterator, Grouped Aggregate, and Grouped Map Pandas UDFs |
| `src/psa/pyspark_udtf.py` | Basic UDTF, lifecycle UDTF (`__init__`/`terminate`), Arrow-optimized UDTF |
| `src/psa/common.py` | Reusable DataFrame transforms (`remove_extra_spaces`, `filter_senior_citizen`) |

## Setup

```bash
# Install dependencies (pick one)
poetry install

# or with pip
pip install pyspark[sql] pyarrow pandas numpy
```

## Enabling Arrow

Set this Spark config to enable Arrow-optimized Pandas ↔ Spark transfers:

```python
spark = (SparkSession.builder
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
         .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
         .getOrCreate())
```

## Running Examples

```bash
python src/psa/pyspark_pyarrow.py
python src/psa/pandas_udf_spark.py
python src/psa/pyspark_udtf.py
```

## Running Tests

```bash
pytest tests/ -v
```
