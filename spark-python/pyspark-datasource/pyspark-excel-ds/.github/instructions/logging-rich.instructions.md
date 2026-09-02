---
applyTo: "{examples/**/*.py,src/pys_excel/_logging.py}"
---

# Logging & Rich Output Instructions

## Library Logging (`pys_excel._logging`)

The library provides a centralized logging system powered by **Rich**.

### Logger hierarchy

All loggers live under the `pys_excel.*` namespace:

```python
from pys_excel._logging import get_logger

logger = get_logger("example.my_script")  # -> pys_excel.example.my_script
```

### Log level control

```bash
# Via environment variable (before import)
PYS_EXCEL_LOG_LEVEL=DEBUG python examples/01_data_source/01_read_basic.py
```

```python
# Programmatically (after import)
from pys_excel import set_log_level

set_log_level("DEBUG")  # DEBUG, INFO, WARNING, ERROR
```

### Logging conventions

- Use `logger.info()` for high-level operations (reading, writing, table
  writes, MERGE INTO).
- Use `logger.debug()` for detailed internals (options, schemas, paths).
- Use `logger.warning()` for recoverable issues (missing optional Delta
  dependency, malformed rows quarantined).
- Use `logger.error()` for failures.
- Always use `%s` formatting (not f-strings) in log calls for lazy
  evaluation:

```python
# Good
logger.info("Reading Excel from %s (options=%s, schema=%s)", path, options, schema)

# Bad — f-string evaluates even when log level is higher
logger.info(f"Reading Excel from {path}")
```

## Rich Print Helpers

```python
from pys_excel import (
    print_header,  # Section header panel
    print_schema,  # Tree-view schema
    print_dataframe,  # Bordered table with auto-columns
    print_success,  # green success message
    print_warning,  # yellow warning message
    print_error,  # red error message
    print_path,  # Labeled file path
    console,  # Raw Rich Console for custom output
)
```

### Example structure with Rich

```python
if __name__ == "__main__":
    spark = get_spark("my-example")

    print_header("1. Section Title")
    print_path("Input", workbook_path)

    df = ExcelReader(spark).sheet("Employees").read(workbook_path)
    print_schema(df, title="Employees Schema")
    print_dataframe(df, title="Employees")
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

Key concepts:
    - Concept 1
    - Concept 2
"""

from pys_excel import (
    generate_sample_workbook,
    get_spark,
    print_dataframe,
    print_header,
    print_path,
    print_schema,
    print_success,
    set_log_level,
)
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.my_example")


if __name__ == "__main__":
    spark = get_spark("my-example")

    print_header("1. Section Title")
    # ... code ...
    print_success("Section complete")

    spark.stop()
```

## Conventions

- Every example file sets `set_log_level("DEBUG")` at module level.
- Every example creates a named logger: `get_logger("example.<name>")`.
- Use `print_header()` for numbered sections in multi-part examples.
- Use `print_schema()` and `print_dataframe()` instead of Spark's built-in
  print methods.
- Always call `spark.stop()` at the end.
- Never use bare `print()` in library code (`src/pys_excel/`) — use the
  logger. `print()` is acceptable in examples but Rich helpers are preferred.
