# Variable & Polymorphic Keys

Handling JSON data where keys are dynamic or vary across records.

## Polymorphic JSON

```python title="examples/06_schema/09_polymorphic_union_schema.py"
--8<-- "examples/06_schema/09_polymorphic_union_schema.py"
```

## Variable Keys

```python title="examples/06_schema/08_map_type_variable_keys.py"
--8<-- "examples/06_schema/08_map_type_variable_keys.py"
```

!!! tip
    For JSON with dynamic keys, consider using `MapType` in your schema or reading
    as a string and parsing with `from_json` using a map schema.

## Run

```bash
python examples/06_schema/09_polymorphic_union_schema.py
python examples/06_schema/08_map_type_variable_keys.py
```
