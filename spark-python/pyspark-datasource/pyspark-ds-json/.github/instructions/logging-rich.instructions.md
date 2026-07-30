---
applyTo: "{**/examples/**/*.py,src/pys_json/_logging.py}"
---

# Logging & Rich Output Instructions

## Library Logging (`pys_json._logging`)

The library provides a centralized logging system powered by **Rich**.

### Logger hierarchy

All loggers live under the `pys_json.*` namespace:

```python
from pys_json._logging import get_logger

logger = get_logger("example.my_script")  # → pys_json.example.my_script
```

### Log level control

```python
# Via environment variable (before import)
# PYS_JSON_LOG_LEVEL=DEBUG python script.py

# Programmatically (after import)
from pys_json import set_log_level
set_log_level("DEBUG")  # DEBUG, INFO, WARNING, ERROR
```

### Logging conventions

- Use `logger.info()` for high-level operations (reading, writing, session creation)
- Use `logger.debug()` for detailed internals (options, schemas, paths)
- Use `logger.warning()` for recoverable issues (corrupt records, type conflicts)
- Use `logger.error()` for failures (invalid codec, type errors)
- Always use `%s` formatting (not f-strings) in log calls for lazy evaluation

```python
# Good
logger.info("Reading JSON from %s (options=%s)", path, options)

# Bad — f-string evaluates even when log level is higher
logger.info(f"Reading JSON from {path}")
```

## Rich Print Helpers

Use Rich helpers for formatted output in examples:

```python
from pys_json import (
    print_header,       # Section header panel (╭───╮)
    print_schema,       # Tree-view schema (├── field: type)
    print_dataframe,    # Bordered table with auto-columns
    print_success,      # ✓ green message
    print_warning,      # ⚠ yellow message
    print_error,        # ✗ red message
    print_path,         # Labeled file path (blue underline)
    console,            # Raw Rich Console for custom output
)
```

### Example structure with Rich

```python
if __name__ == "__main__":
    spark = get_spark("my-example")

    print_header("1. Section Title")
    print_path("Input", data_file)

    df = spark.read.schema(schema).json(data_file)
    print_schema(df, title="My Schema")
    print_dataframe(df, title="Results")
    print_success("Operation completed successfully")

    spark.stop()
```

### When to use what

| Tool | Use for |
|------|---------|
| `print_header()` | Major section separators in multi-part examples |
| `print_schema()` | Always use instead of `df.printSchema()` |
| `print_dataframe()` | Always use instead of `df.show()` |
| `print_path()` | File paths (input/output) |
| `print_success/warning/error()` | Result status messages |
| `logger.info/debug()` | Operational details and metadata |
| `console.print()` | Custom Rich markup output |

## Example File Template

```python
"""Short title — one line description.

Longer explanation of what this example demonstrates.

Key concepts:
    - Concept 1
    - Concept 2

Reference:
    https://spark.apache.org/docs/latest/...
"""

from pys_json import (
    DATA_HOME,
    get_spark,
    print_dataframe,
    print_header,
    print_path,
    print_schema,
    print_success,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.my_example")


if __name__ == "__main__":
    spark = get_spark("my-example")

    # === Section 1 ===
    print_header("1. Section Title")
    # ... code ...
    print_success("Section complete")

    spark.stop()
```

## Conventions

- Every example file sets `set_log_level("DEBUG")` at module level
- Every example creates a named logger: `get_logger("example.<name>")`
- Use `print_header()` for numbered sections in multi-part examples
- Use `print_schema()` and `print_dataframe()` instead of Spark's built-in print methods
- Always call `spark.stop()` at the end
- Never use `print()` directly — use Rich helpers or logger
