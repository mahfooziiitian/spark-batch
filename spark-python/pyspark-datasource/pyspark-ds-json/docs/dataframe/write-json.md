# Write JSON

Write DataFrames to JSON: modes (overwrite, append, ignore), compression, partitioning.

## Usage

```python title="examples/03_dataframe/04_write_json.py"
--8<-- "examples/03_dataframe/04_write_json.py"
```

## Write Modes

| Mode | Behavior |
|------|----------|
| `overwrite` | Replace existing data |
| `append` | Add to existing data |
| `ignore` | Skip if path exists |
| `errorifexists` | Fail if path exists (default) |

## Run

```bash
python examples/03_dataframe/04_write_json.py
```
