---
applyTo: "examples/**/*.py"
---

# Examples Instructions

## Structure

Examples are organized in numbered directories by topic:

```
examples/
├── 01_data_source/        # Basic read/write, all-sheets, distributed spark-excel I/O
├── 02_table_integration/   # excel_to_table, table_to_excel, Delta MERGE upsert
├── 03_properties/          # Header/skiprows, sheet selection, NA/dtypes, formatting
├── 04_schema/               # Explicit schema vs. inference
└── 05_error_handling/       # Missing file, malformed rows
```

## File Naming

- Use numbered prefixes within categories: `01_`, `02_`, etc.
- Names should be descriptive and concise (no `excel_` prefix, no `spark_` prefix
  except when specifically about the spark-excel connector, e.g.
  `06_spark_excel_distributed_io.py`).
- Use snake_case.

## Required Boilerplate

Every example file must:

1. Have a module-level docstring with key concepts (and references if
   relevant, e.g. spark-excel/Databricks docs links).
2. Import from `pys_excel` (not raw pyspark boilerplate).
3. Set `set_log_level("DEBUG")` at module level.
4. Create a named logger via `get_logger("example.<name>")`.
5. Use `if __name__ == "__main__":` guard.
6. Call `spark.stop()` at the end.

```python
"""Title — short description.

Key concepts:
    - Concept 1
    - Concept 2
"""

from pys_excel import get_spark, print_header, print_dataframe, set_log_level
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.name")


if __name__ == "__main__":
    spark = get_spark("example-name")

    # ... example code ...

    spark.stop()
```

## Output Conventions

- Use `print_header()` for section titles (not `print("===")`).
- Use `print_schema()` instead of `df.printSchema()`.
- Use `print_dataframe()` instead of `df.show()`.
- Use `print_path()` for file paths.
- Use `print_success/warning/error()` for status messages.
- Use `logger.info/debug()` for operational context.

## Data Files

- Generate sample workbooks inline with `generate_sample_workbook()` from
  `pys_excel.config` — never commit binary `.xlsx` fixtures.
- Use `DATA_HOME`/`data_path()`/`output_path()`/`temp_excel_path()` for all
  file paths — never hardcode absolute paths.
- Each example should be self-contained: generate any input data it needs at
  the start of the script.

## Delta-Dependent Examples

Examples that require Delta Lake (e.g.
`02_table_integration/03_upsert_table_from_excel.py`) must check availability
and skip gracefully rather than crash when `delta-spark` isn't installed:

```python
import importlib.util

if importlib.util.find_spec("delta") is None:
    print_warning("delta-spark not installed — skipping. Install with: uv sync --extra delta")
    raise SystemExit(0)
```

## Multi-Section Examples

For examples with multiple demonstrations, number the sections:

```python
print_header("1. Basic Usage")
# ...
print_header("2. Advanced Options")
# ...
print_header("3. Edge Cases")
# ...
```

## Verifying Examples

Run the full example suite after any change to `src/pys_excel/` or the
examples themselves:

```bash
./scripts/run-all-examples.sh
```
