# pyspark-excel-ds

Read Excel workbooks into Spark, write Spark DataFrames/tables back to Excel,
and load or upsert Excel extracts into governed Spark tables (Delta/Parquet) —
via a reusable library, runnable examples, and full documentation.

Built for the "Data Architect" workflow: turn ad-hoc Excel extracts from
business users into governed, queryable Spark tables, and turn tables/queries
back into Excel reports.

## Two ways to read/write Excel

1. **Pandas bridge** (`ExcelReader`/`ExcelWriter`) — zero JVM dependency,
   driver-collected, works anywhere Spark runs.
2. **Distributed I/O** (`pys_excel.spark_excel`) — the community
   [spark-excel](https://github.com/crealytics/spark-excel) connector
   (`com.crealytics:spark-excel_2.12:3.5.1_0.20.4`) or Databricks' built-in
   `excel` format (DBR 17.1+), for cluster-scale workbooks.

## Quick start

```bash
uv sync --group dev              # install core + dev dependencies
uv sync --extra delta            # optional: enable Delta Lake / MERGE INTO support

uv run python examples/01_data_source/01_read_basic.py
uv run pytest
```

```python
from pys_excel import ExcelReader, excel_to_table, get_spark

spark = get_spark("excel-quickstart")

# Read a sheet into a DataFrame
df = ExcelReader(spark).sheet("Employees").header(0).read("employees.xlsx")

# Or load straight into a governed table
excel_to_table(spark, "employees.xlsx", "sales.employees", sheet_name="Employees", file_format="delta")

spark.stop()
```

## Project structure

```text
pyspark-excel-ds/
├── src/pys_excel/       # Reusable library: reader, writer, table, spark_excel, config, logging
├── examples/            # Runnable demos organized by topic (see examples/README.md)
├── tests/               # pytest suite mirroring src/pys_excel/
├── docs/                # MkDocs Material documentation site
├── scripts/             # Sample data generation, run-all-examples helper
├── workflows/           # Databricks job task scripts (spark_python_task)
├── resources/           # Databricks Asset Bundle job definitions
├── databricks.yml       # Databricks Asset Bundle root config
├── pyproject.toml       # uv/hatchling build & dependency configuration
└── mkdocs.yml           # Documentation site navigation/config
```

## Deploying to Databricks (Asset Bundles)

A [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/index.html)
at the project root deploys an `excel_ingestion_job` that consumes **two
different libraries** on one job cluster: this project's own `pys_excel`
wheel and the community `com.crealytics:spark-excel` Maven library.

```bash
databricks bundle validate --strict --profile <PROFILE>
databricks bundle deploy -t dev --profile <PROFILE>
databricks bundle run excel_ingestion_job -t dev --profile <PROFILE>
```

See [docs/databricks-runtime/dab-workflow.md](docs/databricks-runtime/dab-workflow.md)
for the full bundle layout, variables, and task breakdown.

## Documentation

Full docs (Getting Started, Data Source, Table Integration, Properties,
Schema, Error Handling, Databricks, Best Practices):

```bash
uv run mkdocs serve   # http://127.0.0.1:8000
```

See [docs/index.md](docs/index.md) for the source, or
[docs/databricks-runtime/index.md](docs/databricks-runtime/index.md) for Databricks Runtime
15.x–17.x+ Excel support guidance.

## Development

```bash
make install        # uv sync --group dev
make test           # pytest
make lint           # ruff check
make format         # ruff format
make type-check     # mypy
make security        # bandit
make docs            # mkdocs build --strict
```

See `Makefile` for the full target list, and see
`.github/copilot-instructions.md` and `.github/instructions/*.md` for
detailed contribution conventions.
