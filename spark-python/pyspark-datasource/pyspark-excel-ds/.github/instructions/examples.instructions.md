---
applyTo: "examples/**/*.py"
---

# Examples Instructions

## Structure

Examples are organized in numbered directories by topic:

```
examples/
├── 01_data_source/        # Read/write basics + distributed spark-excel I/O
│   ├── pandas/             # ExcelReader/ExcelWriter (driver-collected, no JVM package)
│   ├── crealytics_local/   # com.crealytics.spark.excel, run locally (get_spark_with_excel_package)
│   ├── databricks/         # Databricks built-in "excel" format only (DBR 17.1+)
│   ├── crealytics_databricks/  # com.crealytics.spark.excel on Databricks (cluster-attached library)
│   └── 09_read_any_file_generic_options.py  # CLI tool spanning both pandas/distributed modes
├── 02_table_integration/   # excel_to_table, table_to_excel, Delta MERGE upsert
├── 03_properties/          # Header/skiprows, sheet selection, NA/dtypes, formatting
├── 04_schema/               # Explicit schema vs. inference
└── 05_error_handling/       # Missing file, malformed rows
```

`01_data_source/` is split into subfolders by execution approach because the
same read/write concepts (basic read, multi-sheet, schema, formatting) are
demonstrated differently depending on which engine loads the workbook:

- `pandas/` — driver-collected via `ExcelReader`/`ExcelWriter`, zero JVM
  dependency, works everywhere.
- `crealytics_local/` — distributed via `com.crealytics.spark.excel`, running
  locally with the Maven package loaded through
  `get_spark_with_excel_package()`.
- `databricks/` — Databricks' **built-in** `excel` format (DBR 17.1+ only, no
  library install). Must check `is_databricks_runtime()` and skip gracefully
  elsewhere.
- `crealytics_databricks/` — `com.crealytics.spark.excel` running on a
  Databricks cluster with the connector **attached as a cluster Maven
  library** (no `get_spark_with_excel_package()` call needed/possible — the
  library is already on the classpath). Must also skip gracefully off-cluster.

A hybrid example that lets the caller pick the mode at runtime (e.g. via a
`--mode` CLI flag) stays at the `01_data_source/` root rather than being
force-fit into one subfolder.

Each subfolder has its own independent `01_`, `02_`, ... numbering.

## File Naming

- Use numbered prefixes within categories: `01_`, `02_`, etc.
- Names should be descriptive and concise (no `excel_` prefix, no `spark_` prefix
  except when specifically about the spark-excel connector, e.g.
  `crealytics_local/01_spark_excel_distributed_io.py`).
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
from pys_excel.logs import get_logger

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
