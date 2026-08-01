# Case Sensitivity Issues

Handling JSON fields that differ only in case (ID vs id vs Id).

## The Problem

```json
{"ID": 1, "Name": "Alice", "STATUS": "active"}
{"id": 2, "name": "Bob", "status": "inactive"}
{"Id": 3, "NAME": "Charlie", "Status": "pending"}
```

Same logical fields with inconsistent casing across records.

## Spark Behavior

| Setting | Behavior | Risk |
|---------|----------|------|
| `caseSensitive=false` (default) | Merges case variants into one column | Chosen column name may be unpredictable |
| `caseSensitive=true` | Creates separate columns (ID, Id, id) | Most records have nulls in "wrong" columns |

```python
# Check current setting
spark.conf.get("spark.sql.caseSensitive")  # "false" by default
```

## Solution: Explicit Schema (Simplest)

With `caseSensitive=false`, schema fields match case-insensitively:

```python
from pyspark.sql.types import LongType, StringType, StructField, StructType

schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("status", StringType(), True),
])

# "ID", "Id", "id" all match schema field "id"
df = spark.read.schema(schema).json(path)
```

!!! success "Recommended"
    Explicit schema + `caseSensitive=false` is the simplest approach.
    All case variants automatically match your canonical field names.

## Coalesce Case Variants

When reading with `caseSensitive=true` (to preserve all variants):

```python
from pyspark.sql import functions as F

spark.conf.set("spark.sql.caseSensitive", "true")
df = spark.read.json(path)

df_clean = df.select(
    F.coalesce(F.col("ID"), F.col("Id"), F.col("id")).alias("id"),
    F.coalesce(F.col("NAME"), F.col("Name"), F.col("name")).alias("name"),
    F.coalesce(F.col("STATUS"), F.col("Status"), F.col("status")).alias("status"),
)
```

## Detect Case Conflicts

```python
spark.conf.set("spark.sql.caseSensitive", "true")
df = spark.read.json(path)

lower_map = {}
for c in df.columns:
    lower_map.setdefault(c.lower(), []).append(c)

conflicts = {k: v for k, v in lower_map.items() if len(v) > 1}
# {'id': ['ID', 'Id', 'id'], 'name': ['NAME', 'Name', 'name'], ...}
```

## UDF Key Normalization

For full control over key transformation:

```python
import json
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

@F.udf(StringType())
def normalize_json_keys(json_str):
    if not json_str:
        return json_str
    obj = json.loads(json_str)
    if isinstance(obj, dict):
        return json.dumps({k.lower(): v for k, v in obj.items()})
    return json_str

raw = spark.read.text(path)
normalized = raw.select(normalize_json_keys(F.col("value")).alias("value"))
```

!!! warning "UDF Performance"
    UDFs run in Python and are slow at scale. Use explicit schema approach
    when possible — UDFs only for complex normalization requirements.

## Full Demo

```python title="examples/06_schema/21_case_sensitivity.py"
--8<-- "examples/06_schema/21_case_sensitivity.py"
```

## Run

```bash
python examples/06_schema/21_case_sensitivity.py
```

## Best Practices

| Practice | Why |
|----------|-----|
| Use explicit schema | Case-insensitive matching handles variants automatically |
| Normalize early | Lowercase all columns at bronze ingestion |
| Detect conflicts | Read with `caseSensitive=true` to audit |
| Document canonical names | Data contracts prevent upstream drift |
| Keep default (`false`) | Simpler code, fewer surprises |
