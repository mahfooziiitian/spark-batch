---
applyTo: "{src/**/*.py,examples/**/*.py, docs/**/*.md}"
---

# PySpark 4 JSON Datasource Patterns

## PySpark Version

This project targets **PySpark 4.x** (Apache Spark 4.0+). Use PySpark 4 APIs and features:

- **Spark Connect** — preferred client mode for Databricks and remote clusters.
- **Python Data Source API v2** — for custom datasource implementations.
- **VARIANT type** — native semi-structured data type (Spark 4.0+).
- **Collations** — string collation support in schemas.
- **Structured Streaming improvements** — async progress tracking, watermark propagation.

## SparkSession Initialization

### Local Mode (examples and tests)

```python
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .appName("json-example")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ... work with spark ...

spark.stop()
```

### Spark Connect (Databricks / remote clusters)

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .remote(os.environ.get("SPARK_CONNECT_URL", "sc://localhost:15002"))
    .appName("json-connect-example")
    .getOrCreate()
)
```

### Databricks Runtime

```python
# On Databricks, SparkSession is pre-configured as `spark`
# No need to create or stop the session
df = spark.read.json("/mnt/data/events.json")
```

## Reading JSON

Two equivalent approaches:

```python
# Direct method (preferred)
df = spark.read.json("path/to/file.json")

# DataSource API
df = spark.read.format("json").load("path/to/file.json")
```

For multi-line JSON (pretty-printed or array-of-objects files):

```python
df = spark.read.option("multiline", "true").json("path/to/file.json")
```

### Databricks — Unity Catalog Volumes

```python
df = spark.read.json("/Volumes/catalog/schema/volume/data.json")
```

### Databricks — Auto Loader (Streaming)

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/tmp/schema")
    .load("/mnt/landing/json/")
)
```

## VARIANT Type (Spark 4.0+)

Spark 4 introduces native VARIANT type for semi-structured data:

```python
from pyspark.sql import functions as F

# Parse JSON string into VARIANT
df = df.withColumn("data", F.parse_json(F.col("raw_json")))

# Extract fields from VARIANT using : operator (SQL) or variant_get
df.selectExpr("data:name::STRING AS name", "data:age::INT AS age")

# Or with DataFrame API
df.select(F.variant_get("data", "$.name", "STRING").alias("name"))
```

## JSON Read Options Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `multiline` | bool | `false` | Parse multi-line JSON |
| `primitivesAsString` | bool | `false` | Infer all primitives as strings |
| `prefersDecimal` | bool | `false` | Infer floats as `DecimalType` |
| `allowComments` | bool | `false` | Allow Java/C++ style comments |
| `allowUnquotedFieldNames` | bool | `false` | Allow unquoted field names |
| `allowSingleQuotes` | bool | `true` | Allow single quotes |
| `allowNumericLeadingZeros` | bool | `false` | Allow leading zeros |
| `allowNonNumericNumbers` | bool | `false` | Allow NaN, Infinity |
| `mode` | string | `PERMISSIVE` | Parse mode: PERMISSIVE, DROPMALFORMED, FAILFAST |
| `columnNameOfCorruptRecord` | string | `_corrupt_record` | Corrupt record column name |
| `dateFormat` | string | `yyyy-MM-dd` | Date format pattern |
| `timestampFormat` | string | `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]` | Timestamp format |
| `timestampNTZFormat` | string | `yyyy-MM-dd'T'HH:mm:ss[.SSS]` | Timestamp NTZ format |
| `timeZone` | string | (session default) | Timezone ID |
| `lineSep` | string | `\n` | Line separator |
| `samplingRatio` | double | `1.0` | Schema inference sampling fraction |
| `dropFieldIfAllNull` | bool | `false` | Drop all-null fields during inference |
| `encoding` | string | `UTF-8` | Character encoding |
| `locale` | string | `en-US` | Locale for parsing |

## JSON Write Options Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `compression` | string | `none` | Codec: gzip, bzip2, deflate, lz4, snappy, zstd, none |
| `dateFormat` | string | `yyyy-MM-dd` | Date format for output |
| `timestampFormat` | string | `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]` | Timestamp format |
| `ignoreNullFields` | bool | `true` | Omit null fields |
| `encoding` | string | `UTF-8` | Output encoding |

## Error Handling Modes

### PERMISSIVE (default)

```python
schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("_corrupt_record", StringType()),
])
df = spark.read.option("mode", "PERMISSIVE").schema(schema).json(path)
```

> **Important:** Spark raises `UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN` if you
> filter or select **only** the `_corrupt_record` column directly from a raw JSON read.
> Always `.cache()` the DataFrame before querying the corrupt record column:
>
> ```python
> df = spark.read.schema(schema).json(path).cache()
> corrupt = df.filter(df._corrupt_record.isNotNull())  # works after cache
> ```

### DROPMALFORMED

```python
df = spark.read.option("mode", "DROPMALFORMED").json(path)
```

### FAILFAST

```python
df = spark.read.option("mode", "FAILFAST").json(path)
```

## Rescued Data Column

> **Note:** `rescueDataColumn` is a **Databricks-only** option. In OSS PySpark,
> use `_corrupt_record` with PERMISSIVE mode, then re-parse corrupt lines with
> a relaxed all-string schema to recover data.

```python
# Databricks only
df = (
    spark.read
    .option("rescuedDataColumn", "_rescued_data")
    .schema(schema)
    .json(path)
)

# OSS PySpark equivalent — use _corrupt_record + from_json recovery
df = spark.read.option("mode", "PERMISSIVE").schema(schema_with_corrupt).json(path).cache()
corrupt = df.filter(df._corrupt_record.isNotNull())
recovered = corrupt.select(F.from_json("_corrupt_record", relaxed_schema).alias("r"))
```

## JSON Functions

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType

# from_json — parse JSON string
schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
df.select(F.from_json(F.col("json_str"), schema).alias("parsed"))

# to_json — serialize struct to JSON string
df.select(F.to_json(F.struct("name", "age")).alias("json_output"))

# json_tuple — extract multiple keys (generator, use in select)
df.select(F.json_tuple(F.col("json_str"), "name", "age"))

# schema_of_json — infer schema DDL from sample
spark.range(1).select(F.schema_of_json(F.lit('{"name":"Alice","age":30}')))

# get_json_object — JSONPath extraction
df.select(F.get_json_object(F.col("json_str"), "$.address.city"))

# json_array_length
df.select(F.json_array_length(F.col("json_array_str")))
```

## Schema Approaches

### StructType class-based
```python
schema = StructType([
    StructField("name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
])
```

### DDL string
```python
df = spark.read.schema("name STRING, age INT").json(path)
```

### JSON schema string
```python
json_schema = '{"type":"struct","fields":[{"name":"name","type":"string"},{"name":"age","type":"integer"}]}'
df = spark.read.schema(json_schema).json(path)
```

## Databricks-Specific Patterns

### Delta Live Tables (DLT) with JSON

```python
import dlt

@dlt.table(comment="Raw JSON events")
def raw_events():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/mnt/landing/events/")
    )

@dlt.table(comment="Parsed events")
@dlt.expect_or_drop("valid_name", "name IS NOT NULL")
def parsed_events():
    return dlt.read_stream("raw_events").select("name", "age", "timestamp")
```

### Unity Catalog — Managed Tables from JSON

```python
df = spark.read.json("/Volumes/catalog/schema/volume/data.json")
df.write.saveAsTable("catalog.schema.json_events")
```

### Databricks SQL — JSON Functions

```sql
-- Parse JSON with schema_of_json_agg (Spark 4.0+)
SELECT from_json(raw, schema_of_json_agg(raw)) AS parsed FROM raw_table;

-- VARIANT type access
SELECT raw:name::STRING, raw:age::INT FROM events;
```

## Compression

Supported codecs for JSON write (includes zstd in Spark 4):

```python
df.write.option("compression", "gzip").json(output_path)
df.write.option("compression", "zstd").json(output_path)   # New in Spark 4
df.write.option("compression", "snappy").json(output_path)
df.write.option("compression", "lz4").json(output_path)
```

## Pandas / Arrow Bridge

```python
import pandas as pd

# Pandas → Spark (uses Arrow by default in PySpark 4)
pdf = pd.read_json("data.json")
df = spark.createDataFrame(pdf)

# Spark → Pandas
pdf = df.toPandas()
```

## Library Usage (src/pys_json)

Prefer the library wrappers for reusable code:

```python
from pys_json import JsonReader, JsonWriter, create_spark_session
from pys_json.parsing import parse_json_column, infer_schema_from_sample
from pys_json.schema import with_corrupt_record

spark = create_spark_session("my-job")
reader = JsonReader(spark).multiline().permissive()
df = reader.read("/data/events.json")

writer = JsonWriter(compression="gzip")
writer.write(df, "/output/events")
spark.stop()
```
