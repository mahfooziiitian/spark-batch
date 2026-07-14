---
applyTo: "src/**/*.py"
---

# PySpark JSON Datasource Patterns

## SparkSession Initialization

Always read the Spark master URL from the `SPARK_MASTER` environment variable, defaulting to `local[*]`. Stop the session at the end of every script.

```python
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .appName("json-example")
    .getOrCreate()
)

# ... work with spark ...

spark.stop()
```

## Reading JSON

Two equivalent approaches:

```python
# Direct method
df = spark.read.json("path/to/file.json")

# DataSource API
df = spark.read.format("json").load("path/to/file.json")
```

For multi-line JSON (pretty-printed or array-of-objects files):

```python
df = spark.read.option("multiline", "true").json("path/to/file.json")
```

## JSON Read Options Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `multiline` | bool | `false` | Parse multi-line JSON (one record may span multiple lines) |
| `primitivesAsString` | bool | `false` | Infer all primitive values as string type |
| `prefersDecimal` | bool | `false` | Infer floating-point values as `DecimalType` instead of `DoubleType` |
| `allowComments` | bool | `false` | Allow Java/C++ style comments in JSON |
| `allowUnquotedFieldNames` | bool | `false` | Allow unquoted JSON field names |
| `allowSingleQuotes` | bool | `true` | Allow single quotes in addition to double quotes |
| `allowNumericLeadingZeros` | bool | `false` | Allow leading zeros in numbers (e.g., `00012`) |
| `allowNonNumericNumbers` | bool | `false` | Allow `NaN`, `Infinity`, `-Infinity` as valid numbers |
| `allowBackslashEscapingAnyCharacter` | bool | `false` | Allow backslash to escape any character |
| `mode` | string | `PERMISSIVE` | Parse mode: `PERMISSIVE`, `DROPMALFORMED`, or `FAILFAST` |
| `columnNameOfCorruptRecord` | string | `_corrupt_record` | Column name for malformed records in PERMISSIVE mode |
| `dateFormat` | string | `yyyy-MM-dd` | Date format pattern (Java `SimpleDateFormat`) |
| `timestampFormat` | string | `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]` | Timestamp format pattern |
| `timestampNTZFormat` | string | `yyyy-MM-dd'T'HH:mm:ss[.SSS]` | Timestamp without timezone format |
| `timeZone` | string | (session default) | Timezone ID for timestamp parsing |
| `lineSep` | string | `\n`, `\r\n`, `\r` | Line separator for JSON Lines files |
| `samplingRatio` | double | `1.0` | Fraction of input to sample for schema inference |
| `dropFieldIfAllNull` | bool | `false` | Drop fields with all null values during schema inference |
| `encoding` | string | `UTF-8` | Character encoding: `UTF-8`, `UTF-16BE`, `UTF-16LE`, `UTF-32BE`, `UTF-32LE` |
| `locale` | string | `en-US` | Locale for number/date parsing (IETF BCP 47 tag) |

## JSON Write Options Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `compression` | string | `none` | Compression codec: `gzip`, `bzip2`, `deflate`, `lz4`, `snappy`, `none` |
| `dateFormat` | string | `yyyy-MM-dd` | Date format for output |
| `timestampFormat` | string | `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]` | Timestamp format for output |
| `ignoreNullFields` | bool | `true` | Omit fields with null values in output |
| `encoding` | string | `UTF-8` | Output character encoding |

## Error Handling Modes

### PERMISSIVE (default)

Puts malformed records into a designated corrupt record column. Define the column in the schema:

```python
schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("_corrupt_record", StringType()),
])
df = spark.read.option("mode", "PERMISSIVE").schema(schema).json(path)
df.filter(df["_corrupt_record"].isNotNull()).show(truncate=False)
```

### DROPMALFORMED

Silently drops rows that cannot be parsed:

```python
df = spark.read.option("mode", "DROPMALFORMED").json(path)
```

### FAILFAST

Throws an exception on the first malformed record:

```python
df = spark.read.option("mode", "FAILFAST").json(path)
```

## Rescued Data Column

Captures columns that do not match the provided schema into a single `_rescued_data` column:

```python
df = (
    spark.read
    .option("rescuedDataColumn", "_rescued_data")
    .schema(schema)
    .json(path)
)
```

## JSON Functions

### from_json — Parse JSON string to structured type

```python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType, ArrayType

# String → StructType
schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
df.select(from_json(col("json_str"), schema).alias("parsed"))

# String → MapType
df.select(from_json(col("json_str"), MapType(StringType(), StringType())).alias("map_data"))

# String → ArrayType
array_schema = ArrayType(StructType([StructField("id", IntegerType())]))
df.select(from_json(col("json_str"), array_schema).alias("items"))
```

### to_json — Convert struct to JSON string

```python
from pyspark.sql.functions import to_json, struct, col

df.select(to_json(struct(col("name"), col("age"))).alias("json_output"))
```

### json_tuple — Extract multiple keys at once

```python
from pyspark.sql.functions import json_tuple

df.select(json_tuple(col("json_str"), "name", "age").alias("name", "age"))
```

### schema_of_json — Infer schema from a sample JSON string

```python
from pyspark.sql.functions import schema_of_json, lit

inferred = spark.range(1).select(schema_of_json(lit('{"name":"Alice","age":30}')))
```

### json_object_keys — Get keys from a JSON object

```python
from pyspark.sql.functions import json_object_keys, col

df.select(json_object_keys(col("json_str")).alias("keys"))
```

### json_array_length — Get length of a JSON array

```python
from pyspark.sql.functions import json_array_length, col

df.select(json_array_length(col("json_array_str")).alias("length"))
```

## Schema Approaches

### StructType class-based

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
])
df = spark.read.schema(schema).json(path)
```

### DDL string

```python
df = spark.read.schema("name STRING, age INT").json(path)
```

### schema_of_json() inference

```python
from pyspark.sql.functions import schema_of_json, lit

schema_str = spark.range(1).select(schema_of_json(lit('{"name":"a","age":1}'))).collect()[0][0]
df = spark.read.schema(schema_str).json(path)
```

### JSON schema string

```python
json_schema = '{"type":"struct","fields":[{"name":"name","type":"string"},{"name":"age","type":"integer"}]}'
df = spark.read.schema(json_schema).json(path)
```

### Python dataclass-based

```python
from dataclasses import dataclass

@dataclass
class Person:
    name: str
    age: int

# Use with Spark ≥ 3.5 — pass the dataclass directly where schema is expected
```

## Encoding

Spark JSON reader supports multiple encodings via the `encoding` option:

- `UTF-8` (default)
- `UTF-16BE` / `UTF-16LE` (with BOM detection)
- `UTF-32BE` / `UTF-32LE`

```python
df = spark.read.option("encoding", "UTF-16BE").json(path)
```

## Compression

Supported codecs for JSON write:

```python
df.write.option("compression", "gzip").json(output_path)    # .json.gz
df.write.option("compression", "bzip2").json(output_path)   # .json.bz2
df.write.option("compression", "deflate").json(output_path)  # .json.deflate
df.write.option("compression", "lz4").json(output_path)     # .json.lz4
df.write.option("compression", "snappy").json(output_path)  # .json.snappy
df.write.option("compression", "none").json(output_path)    # .json (uncompressed)
```

## JSONPath Expressions

Use `get_json_object` to extract values with JSONPath:

```python
from pyspark.sql.functions import get_json_object, col

df.select(get_json_object(col("json_str"), "$.store.book[0].title").alias("title"))
```

## Pandas Bridge

Convert a Pandas DataFrame (loaded from JSON) into a Spark DataFrame:

```python
import pandas as pd

pdf = pd.read_json("path/to/file.json")
df = spark.createDataFrame(pdf)
```

## Sample Data Creation

Create temporary JSON files for testing using Python dicts:

```python
import json
from pathlib import Path

data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
path = Path("output/sample.json")
path.parent.mkdir(parents=True, exist_ok=True)
with path.open("w") as f:
    for record in data:
        f.write(json.dumps(record) + "\n")
```
