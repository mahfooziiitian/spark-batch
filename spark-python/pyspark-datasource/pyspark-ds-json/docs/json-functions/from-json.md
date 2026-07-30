# from_json

Parse a JSON string column into a structured type (struct, map, or array).

## Signature

```python
from_json(col, schema, options={})
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `col` | Column or str | Column containing JSON strings |
| `schema` | StructType, ArrayType, MapType, or str | Target schema |
| `options` | dict | Optional JSON parsing options |

## String to Struct

The most common pattern — parse a JSON string into named fields:

```python title="examples/02_json_functions/01_from_json.py"
--8<-- "examples/02_json_functions/01_from_json.py"
```

## Run

```bash
python examples/02_json_functions/01_from_json.py
```
