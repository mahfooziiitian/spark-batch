# schema_of_json

Infer the schema (as a DDL string) from a sample JSON string at runtime.

## Usage

```python title="examples/02_json_functions/05_schema_of_json.py"
--8<-- "examples/02_json_functions/05_schema_of_json.py"
```

!!! note
    `schema_of_json` accepts a single JSON string literal — not a column reference.
    Use it to dynamically generate schemas for `from_json`.

## Run

```bash
python examples/02_json_functions/05_schema_of_json.py
```
