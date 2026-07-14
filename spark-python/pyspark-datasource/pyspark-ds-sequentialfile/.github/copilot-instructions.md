# Copilot Instructions — pyspark-ds-sequentialfile

This project demonstrates reading, writing, and managing **Hadoop SequenceFiles**
using PySpark's SparkContext RDD API.

> **Note:** SequenceFile is a Hadoop-native binary key-value format. It is not
> a Spark datasource — it is accessed via the SparkContext RDD API, not
> `spark.read.format(...)`.

## Technology Stack

| Component | Version / Tool |
|-----------|---------------|
| Python | ≥ 3.11 |
| PySpark | 3.5.x |
| Package manager | uv (preferred) |
| Testing | pytest ≥ 8.0 |

## SequenceFile Patterns

### Reading SequenceFiles

```python
rdd = spark.sparkContext.sequenceFile(
    "/path/to/sequencefile",
    keyClass="org.apache.hadoop.io.Text",
    valueClass="org.apache.hadoop.io.Text",
)
```

### Common Key/Value Classes

| Class | Description |
|-------|-------------|
| `org.apache.hadoop.io.Text` | UTF-8 string |
| `org.apache.hadoop.io.IntWritable` | 32-bit integer |
| `org.apache.hadoop.io.LongWritable` | 64-bit integer |
| `org.apache.hadoop.io.BytesWritable` | Raw bytes |

### Converting RDD to DataFrame

```python
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType

# Via Row objects
df = rdd.map(lambda kv: Row(key=kv[0], value=kv[1])).toDF()

# Via explicit schema
schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True),
])
df = spark.createDataFrame(rdd, schema)
```

### Writing SequenceFiles

```python
rdd = spark.sparkContext.parallelize([("key1", "value1"), ("key2", "value2")])
rdd.saveAsSequenceFile("/output/sequencefile")
```

### Compression

SequenceFile supports block-level and record-level compression natively.
Compression is configured via Hadoop configuration properties:

```python
spark.sparkContext._jsc.hadoopConfiguration().set(
    "io.seqfile.compression.type", "BLOCK"
)
```

## Conventions

- Use `SPARK_MASTER` env var with `local[*]` fallback.
- `from pyspark.sql import functions as F` — never `import *`.
- Always call `spark.stop()` at the end of standalone scripts.
- Convert RDDs to DataFrames early — prefer DataFrame API over RDD API.

## Things to Avoid

- Do not use `from pyspark.sql.functions import *`.
- Do not omit `spark.stop()` in standalone scripts.
- Do not use `len(df.collect())` — use `df.count()`.
- Do not stay in the RDD API when DataFrame operations are possible.
