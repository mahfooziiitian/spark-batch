---
applyTo: "**/*.py"
---

# PySpark MongoDB — Python Code Instructions

## SparkSession Pattern

All scripts must configure the MongoDB Spark Connector via `spark.jars.packages` and
use environment variables with sensible defaults for connection URIs:

```python
import os
from pyspark.sql import SparkSession

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DB = os.environ.get("MONGO_DB", "tutorial")

spark = (
    SparkSession.builder
    .appName("pyspark-mongodb-example")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .config(
        "spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector_2.13:10.1.1",
    )
    .config("spark.mongodb.read.connection.uri", MONGO_URI)
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

### Key rules

- **Connector JAR**: Always load via `spark.jars.packages` — never download JARs manually.
- **Connection URIs**: Read from `MONGO_URI` env var with `mongodb://127.0.0.1:27017` fallback.
- **Database & collection**: Pass as `.option("database", ...)` and `.option("collection", ...)` on read/write — not embedded in the URI.
- Set `spark.sql.adaptive.enabled` to `"true"` for non-trivial jobs.

## Imports

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F       # always alias as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
```

Never use `from pyspark.sql.functions import *`.

## Environment Variables

Scripts that need connection details use env vars with safe fallbacks:

```python
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DB  = os.environ.get("MONGO_DB", "tutorial")
```

## MongoDB Read / Write Patterns

### Writing to MongoDB

```python
df.write.format("mongodb") \
    .mode("append") \
    .option("database", "tutorial") \
    .option("collection", "people_out") \
    .save()
```

### Reading from MongoDB

```python
df = (
    spark.read.format("mongodb")
    .option("database", "tutorial")
    .option("collection", "people")
    .load()
)
```

### Write modes

| Mode        | When to use                             |
| ----------- | --------------------------------------- |
| `append`    | Add rows without touching existing data |
| `overwrite` | Replace the entire collection           |

## DataFrame Creation

Use `spark.createDataFrame()` with explicit column names for sample data:

```python
df = spark.createDataFrame(
    [("Alice", 30), ("Bob", 25)],
    ["name", "age"],
)
```

## Script Structure

Every standalone script follows this order:

1. Imports
2. Environment variable reads
3. SparkSession creation (with MongoDB connector config)
4. Business logic (create / transform / read / write DataFrames)
5. `spark.stop()`

## Style

- **No boilerplate comments.** Only comment code that needs clarification.
- Use `from pyspark.sql import functions as F` — never `import *`.
- Always call `spark.stop()` at the end of standalone scripts.
- Prefer method chaining for DataFrame transformations.
