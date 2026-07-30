# to_json

Convert a struct, map, or array column to a JSON string.

## Signature

```python
to_json(col, options={})
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `col` | Column | A struct, map, or array column |
| `options` | dict | Optional JSON formatting options (e.g., `dateFormat`, `timestampFormat`) |

## Usage

```python title="examples/02_json_functions/02_to_json.py"
--8<-- "examples/02_json_functions/02_to_json.py"
```

## Common Patterns

### Struct to JSON

```python
from pyspark.sql.functions import to_json, struct

df.withColumn("json_str", to_json(struct("name", "age", "city")))
```

### Map to JSON

```python
from pyspark.sql.functions import to_json, col

df.withColumn("json_str", to_json(col("map_column")))
```

### With Formatting Options

```python
df.withColumn("json_str", to_json(col("data"), {"dateFormat": "yyyy/MM/dd"}))
```

!!! tip
    `to_json` is useful for serializing structured data back to JSON for Kafka producers,
    REST APIs, or downstream JSON-consuming systems.

## Run

```bash
python examples/02_json_functions/02_to_json.py
```
