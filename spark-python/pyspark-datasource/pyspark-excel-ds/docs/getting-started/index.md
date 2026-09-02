# Getting Started

Set up your environment and run your first PySpark Excel example in under 5 minutes.

## Prerequisites

| Component | Version | Notes |
|-----------|---------|-------|
| Python | ≥ 3.11 | 3.12 also supported |
| Java | 17+ | Required by PySpark |
| PySpark | 3.5.x | Installed via pip/uv |
| Delta Lake (optional) | 3.2.x | Only needed for `upsert_table_from_excel` |

!!! warning "Java 17 Required"
    PySpark requires Java 17 or later. Set `JAVA_HOME` to your Java 17 installation.

## Installation

=== "uv (Recommended)"
    ```bash
    git clone https://github.com/mahfooziiitian/spark-batch.git
    cd spark-batch/spark-python/pyspark-datasource/pyspark-excel-ds

    uv sync --group dev
    # Optional: MERGE INTO upsert support
    uv sync --group dev --extra delta
    ```

=== "pip"
    ```bash
    git clone https://github.com/mahfooziiitian/spark-batch.git
    cd spark-batch/spark-python/pyspark-datasource/pyspark-excel-ds

    python -m venv .venv
    source .venv/bin/activate
    pip install -e ".[delta]"
    ```

## Project Structure

```text
pyspark-excel-ds/
├── src/pys_excel/          # Reusable library
│   ├── reader/             # ExcelReader (pandas bridge)
│   ├── writer/             # ExcelWriter (pandas bridge)
│   ├── table/               # excel_to_table / table_to_excel / upsert_table_from_excel
│   ├── spark_excel.py      # Distributed I/O via spark-excel / Databricks native
│   └── config.py           # Environment configuration
├── examples/               # Runnable examples (5 categories)
├── docs/                   # MkDocs Material documentation
├── tests/                  # pytest test suite
└── Makefile                # Common dev commands
```

## Run Your First Example

```bash
uv run python examples/01_data_source/01_read_basic.py
```

Expected output (Rich formatted):

```text
╭──────────────────────────────────────────────────────────╮
│                1. Read the 'Employees' sheet               │
╰──────────────────────────────────────────────────────────╯
┏━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
┃ emp_id ┃ name  ┃ department  ┃ salary  ┃ hire_date           ┃
┡━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
│ 1      │ Alice │ Engineering │ 95000.0 │ 2019-03-01 00:00:00 │
│ 2      │ Bob   │ Sales       │ 72000.0 │ 2020-07-15 00:00:00 │
└────────┴───────┴─────────────┴─────────┴─────────────────────┘
```

## Using the Library

```python
from pys_excel import ExcelReader, ExcelWriter, get_spark, print_dataframe

spark = get_spark("my-app")  # Configures Java, log level, Hive support automatically

df = ExcelReader(spark).sheet("Employees").read("data/employees.xlsx")
print_dataframe(df, title="Employees")

ExcelWriter(sheet_name="Report").write(df.limit(10), "output/report.xlsx")

spark.stop()
```

## The Core Table Workflow

```python
from pys_excel import excel_to_table, table_to_excel, get_spark

spark = get_spark("excel-table-workflow")

# 1. Land an Excel extract as a Spark table
excel_to_table(spark, "data/employees.xlsx", "sales.employees", sheet_name="Employees")

# 2. Query it like any other table
spark.sql("SELECT department, COUNT(*) FROM sales.employees GROUP BY department").show()

# 3. Export a report back to Excel
table_to_excel(spark, "sales.employees", "output/employees_report.xlsx")

spark.stop()
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|--------------|
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `JAVA_HOME_17` | — | Path to Java 17 installation |
| `PYS_EXCEL_LOG_LEVEL` | `INFO` | Library log level (DEBUG, INFO, WARNING) |
| `DATA_HOME` | `<project>/data` | Default data directory |
| `DATABRICKS_RUNTIME_VERSION` | — | Auto-set on Databricks; used by `resolve_excel_format()` |

## Make Commands

```bash
make help          # Show all available commands
make format        # Format code with ruff
make lint          # Run ruff linter
make type-check     # Run mypy type checking
make test           # Run pytest
make check-all      # Run all checks (format, lint, mypy, bandit)
make docs           # Build documentation
make docs-serve     # Serve docs locally at localhost:8000
```

## Next Steps

<div class="grid cards" markdown>

-   :material-file-document:{ .lg .middle } **[Data Source](../data-source/index.md)**

    ---

    Learn to read and write Excel files

-   :material-table:{ .lg .middle } **[Table Integration](../table-integration/index.md)**

    ---

    Land Excel extracts as Spark tables and export tables back to Excel

-   :material-code-braces:{ .lg .middle } **[Schema](../schema/index.md)**

    ---

    Define, infer, and validate schemas

-   :simple-databricks:{ .lg .middle } **[Databricks](../databricks-runtime/index.md)**

    ---

    Run on Databricks Runtime 15.x+ (spark-excel) or 17.1+ (built-in Excel)

</div>
