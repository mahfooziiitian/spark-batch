# DROPMALFORMED Mode

Silently drops any rows that cannot be parsed according to the schema.

## Usage

```python title="examples/05_error_handling/01_error_modes.py"
--8<-- "examples/05_error_handling/01_error_modes.py"
```

!!! warning
    Data is silently lost with this mode. Use only when you are certain that malformed
    records are not valuable for debugging or auditing.

## Run

```bash
python examples/05_error_handling/01_error_modes.py
```
