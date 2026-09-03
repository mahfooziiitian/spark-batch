---
applyTo: "src/**/*.py,examples/**/*.py"
---

# PySpark + Native XML Conventions

These rules apply to both **library code** (`src/spark_xml/`) and **example
scripts** (`examples/`). See `copilot-instructions.md` for the folder split.

## SparkSession

Every standalone example builds its own session using the `SPARK_MASTER` env
var with a `local[*]` fallback:

```python
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("spark-xml")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))  # (1)!
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

1. Falls back to local mode when `SPARK_MASTER` is not set.

- **No JARs.** Do not add `spark.jars.packages` for XML — the `xml` source is
  built into Spark 4.
- Always call `spark.stop()` at the end of a standalone script.

## Reading & Writing XML Files

```python
df = (
    spark.read.format("xml")      # built-in Spark 4 source — not com.databricks.spark.xml
    .option("rowTag", "book")
    .load(path)
)

df.write.format("xml").option("rootTag", "books").option("rowTag", "book").save(out)
```

## Parsing an XML String Column

Use the native `from_xml` / `schema_of_xml` functions (no JVM bridge):

```python
from pyspark.sql.functions import from_xml, schema_of_xml, lit

options = {"rowTag": "Level_0"}
sample = df.select("content").first()["content"]          # a representative row
schema = df.select(schema_of_xml(lit(sample), options)).first()[0]
parsed = df.withColumn("parsed", from_xml(df.content, schema, options))
```

- `schema_of_xml` takes an XML **literal** (wrap the sample with `lit(...)`).
- Prefer passing a `StructType` you define explicitly for stable schemas;
  infer with `schema_of_xml` only for exploratory examples.

## Options Reference (common)

| Option | Purpose |
|--------|---------|
| `rowTag` | Element treated as a row (required for read/parse). |
| `rootTag` | Wrapper element when writing. |
| `attributePrefix` | Prefix for attributes (default `_`). |
| `valueTag` | Field name for element character data (default `_VALUE`). |
| `mode` | Parse mode: `PERMISSIVE`, `DROPMALFORMED`, `FAILFAST`. |

## Things to Avoid

- Do **not** use `.format("com.databricks.spark.xml")` — use `.format("xml")`.
- Do **not** call the JVM `_jvm.com.databricks...` `from_xml` bridge — use the
  native `pyspark.sql.functions.from_xml`.
- Do **not** import one example script from another — each is self-contained.
- Do **not** use `from pyspark.sql.functions import *`.
