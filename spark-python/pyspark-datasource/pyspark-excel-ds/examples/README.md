# Examples

Standalone PySpark Excel examples organized by topic. Each script is
independently runnable and self-contained (generates its own sample data).

## Prerequisites

```bash
uv sync --group dev            # core + dev dependencies
uv sync --extra delta          # optional, needed for 02_table_integration/03_upsert_table_from_excel.py
```

## Structure

```text
examples/
├── 01_data_source/        # Basic read/write, all-sheets, distributed spark-excel I/O
├── 02_table_integration/   # excel_to_table, table_to_excel, Delta MERGE upsert
├── 03_properties/          # Header/skiprows, sheet selection, NA/dtypes, formatting
├── 04_schema/               # Explicit schema vs. schema inference
└── 05_error_handling/       # Missing file, malformed rows
```

| Directory | Examples |
|-----------|----------|
| `01_data_source/` | `01_read_basic.py`, `02_read_multiple_sheets.py`, `03_read_with_schema.py`, `04_write_basic.py`, `05_write_multiple_sheets.py`, `06_spark_excel_distributed_io.py` |
| `02_table_integration/` | `01_excel_to_table.py`, `02_table_to_excel.py`, `03_upsert_table_from_excel.py` |
| `03_properties/` | `01_header_and_skiprows.py`, `02_sheet_selection.py`, `03_na_values_and_dtypes.py`, `04_formatting_and_styles.py` |
| `04_schema/` | `01_explicit_schema.py`, `02_schema_inference.py` |
| `05_error_handling/` | `01_missing_file.py`, `02_malformed_rows.py` |

## Running

```bash
# Run any example directly
uv run python examples/01_data_source/01_read_basic.py
uv run python examples/02_table_integration/01_excel_to_table.py

# Or run the full suite
./scripts/run-all-examples.sh
```

All examples use the `pys_excel` library for session management and
configuration:

```python
from pys_excel import get_spark, generate_sample_workbook

spark = get_spark("my-example")
workbook = generate_sample_workbook()
```
