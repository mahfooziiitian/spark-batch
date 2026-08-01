# Mixed Root Structures

Handling JSON files with inconsistent root types (objects, arrays, primitives).

## The Problem

Input files have different root structures:

=== "File 1 — Object (JSON Lines)"
    ```json
    {"id": 1, "name": "Alice"}
    {"id": 2, "name": "Bob"}
    ```

=== "File 2 — Array"
    ```json
    [{"id": 3, "name": "Charlie"}, {"id": 4, "name": "Diana"}]
    ```

=== "File 3 — Primitive"
    ```json
    "just a string"
    ```

!!! failure "Why This Fails"
    Spark expects consistent root type across all files. Reading mixed roots
    together produces unexpected results or errors.

## Solution: Text → Classify → Parse

```mermaid
graph LR
    A[spark.read.text] --> B[Classify root type]
    B -->|startsWith '{'| C[Parse as object]
    B -->|startsWith '['| D[Parse as array]
    B -->|otherwise| E[Skip / log]
    C --> F[Union]
    D -->|explode| F
```

### Step 1: Read as text

```python
raw_df = spark.read.text(paths)
```

### Step 2: Classify by root type

```python
from pyspark.sql import functions as F

classified = raw_df.withColumn(
    "root_type",
    F.when(F.trim(F.col("value")).startsWith("{"), "object")
    .when(F.trim(F.col("value")).startsWith("["), "array")
    .otherwise("skip"),
)
```

### Step 3: Parse each type separately

```python
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
])

# Objects
df_objects = (
    classified.filter(F.col("root_type") == "object")
    .select(F.from_json(F.col("value"), schema).alias("data"))
    .select("data.*")
)

# Arrays
df_arrays = (
    classified.filter(F.col("root_type") == "array")
    .select(F.from_json(F.col("value"), ArrayType(schema)).alias("data"))
    .select(F.explode_outer("data").alias("item"))
    .select("item.*")
)

# Union
df_final = df_objects.unionByName(df_arrays)
```

### Step 4: Log skipped records

```python
skipped = classified.filter(F.col("root_type") == "skip")
logger.warning("Skipped %s non-parseable lines", skipped.count())
```

## Alternative: `multiLine` for Array Files

If **all** files are array-rooted (no JSON Lines mix):

```python
df = spark.read.option("multiLine", "true").schema(schema).json(path)
```

!!! warning
    `multiLine=true` treats the entire file as one JSON value. It cannot
    be combined with JSON Lines files in the same read.

## Full Demo

```python title="examples/06_schema/19_mixed_root_structures.py"
--8<-- "examples/06_schema/19_mixed_root_structures.py"
```

## Run

```bash
python examples/06_schema/19_mixed_root_structures.py
```

## Decision Table

| Root Type | Detection | Parse Strategy |
|-----------|-----------|----------------|
| Object `{...}` | `startsWith("{")` | `from_json(value, StructType)` |
| Array `[...]` | `startsWith("[")` | `from_json(value, ArrayType)` + `explode` |
| Primitive | `otherwise` | Skip or log — cannot structure |
| null / empty | `otherwise` | Skip |

!!! tip "Production Best Practice"
    Always log skipped records with their content for investigation.
    A sudden spike in skipped records indicates upstream data issues.
