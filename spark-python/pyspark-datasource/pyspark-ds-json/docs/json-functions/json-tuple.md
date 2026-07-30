# json_tuple

Extract multiple top-level keys from a JSON string column in a single pass — more efficient
than multiple `get_json_object` calls.

## Signature

```python
json_tuple(col, *fields)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `col` | Column or str | Column containing JSON strings |
| `*fields` | str | One or more key names to extract |

## Usage

```python title="examples/02_json_functions/03_json_tuple.py"
--8<-- "examples/02_json_functions/03_json_tuple.py"
```

## Key Differences from get_json_object

| Feature | `json_tuple` | `get_json_object` |
|---------|-------------|-------------------|
| Multiple keys | ✅ Single parse | ❌ One parse per call |
| Nested paths | ❌ Top-level only | ✅ Full JSONPath |
| Return type | Generator (use with `select`) | String column |
| Performance | Better for 3+ keys | Better for 1-2 keys with paths |

!!! tip "Performance"
    `json_tuple` parses the JSON string only once regardless of how many keys you extract.
    Use it when you need 3 or more top-level fields.

!!! warning
    `json_tuple` is a generator function — it must be used in `select()`, not `withColumn()`.
    Rename the output columns using `.toDF()`.

## Run

```bash
python examples/02_json_functions/03_json_tuple.py
```
