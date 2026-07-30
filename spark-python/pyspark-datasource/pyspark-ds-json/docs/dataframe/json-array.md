# JSON Arrays

Loading and processing JSON files that contain arrays.

## Basic JSON Array

```python title="examples/01_data_source/read_json_array.py"
--8<-- "examples/01_data_source/read_json_array.py"
```

## Array Structure

```python title="examples/01_data_source/read_json_array_structure.py"
--8<-- "examples/01_data_source/read_json_array_structure.py"
```

!!! tip
    Use `option("multiline", "true")` when reading JSON files that contain a top-level array
    spanning multiple lines.

## Run

```bash
python examples/01_data_source/read_json_array.py
python examples/01_data_source/read_json_array_structure.py
```
