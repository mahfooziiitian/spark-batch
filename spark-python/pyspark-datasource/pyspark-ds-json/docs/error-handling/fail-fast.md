# FAILFAST Mode

Throws an exception immediately when a malformed record is encountered.

## Usage

```python title="examples/05_error_handling/01_error_modes.py"
--8<-- "examples/05_error_handling/01_error_modes.py"
```

!!! tip
    Use `FAILFAST` during development and testing to catch data quality issues early.
    Switch to `PERMISSIVE` in production pipelines.

## Run

```bash
python examples/05_error_handling/01_error_modes.py
```
