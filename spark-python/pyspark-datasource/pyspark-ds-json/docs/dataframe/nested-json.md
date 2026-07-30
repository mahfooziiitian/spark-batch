# Nested JSON

Working with nested JSON structures: structs, arrays, maps, and flattening patterns.

## Usage

```python title="examples/03_dataframe/03_nested_json.py"
--8<-- "examples/03_dataframe/03_nested_json.py"
```

!!! tip "Accessing Nested Fields"
    Use dot notation for struct fields: `df.select("address.city")`.
    Use `explode()` for arrays: `df.select(F.explode("items").alias("item"))`.

## Run

```bash
python examples/03_dataframe/03_nested_json.py
```
