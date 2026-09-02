# Deploying with Databricks Asset Bundles

The `databricks.yml` bundle at the project root packages this library into a
runnable Databricks Job — no manual notebook wiring required. It demonstrates
consuming **two different libraries on one cluster**: this project's own
`pys_excel` wheel and the third-party
[spark-excel](https://github.com/crealytics/spark-excel) Maven library.

## Bundle layout

```text
pyspark-excel-ds/
├── databricks.yml                          # bundle root: artifacts, variables, targets
├── resources/
│   └── excel_ingestion_job.job.yml         # job/cluster/task/library definitions
└── workflows/
    ├── ingest_excel_to_delta.py            # pandas bridge (pys_excel wheel only)
    ├── distributed_ingest_spark_excel.py   # spark-excel Maven library (+ wheel)
    └── export_table_to_excel.py            # Delta table -> Excel report (pandas bridge)
```

## What the job does

`excel_ingestion_job` runs on a single shared job cluster
(`spark_version: 15.4.x-scala2.12`) with three tasks:

1. **`ingest_excel_to_delta`** — loads an Excel extract into a Delta table
   using the pandas-bridge `excel_to_table()` from the `pys_excel` wheel.
2. **`distributed_ingest_spark_excel`** — loads the same style of workbook at
   cluster scale using `pys_excel.spark_excel.read_spark_excel()`, which
   drives the `com.crealytics:spark-excel_2.12:3.5.1_0.20.4` Maven library
   attached alongside the wheel — the "different library" combination.
3. **`export_table_to_excel`** (depends on task 1) — writes the ingested
   Delta table back out to an Excel report using `table_to_excel()`.

```yaml
libraries:
  - whl: ../dist/*.whl
  - maven:
      coordinates: "com.crealytics:spark-excel_2.12:3.5.1_0.20.4"
```

The bundle's `artifacts:` block builds the wheel automatically on every
deploy:

```yaml
artifacts:
  pys_excel:
    type: whl
    build: uv build --wheel
    path: .
```

## Configuring for your workspace

Adjust the bundle variables in `databricks.yml` (or override per-target) to
point at your own Unity Catalog catalog/schema and Volumes paths:

| Variable | Default | Purpose |
|---|---|---|
| `catalog` | `main` | Unity Catalog catalog for demo tables |
| `schema` | `excel_demo` (`excel_demo_dev` in `dev`) | Unity Catalog schema |
| `input_volume_path` | `/Volumes/main/excel_demo/landing` | Source Excel workbooks |
| `output_volume_path` | `/Volumes/main/excel_demo/reports` | Exported Excel reports |
| `node_type_id` | `i4i.xlarge` | Job cluster node type |
| `policy_id` | _(lookup: `job_workflow_cluster_oz`)_ | Cluster policy applied to the job cluster, resolved by name |

## Deploying and running

!!! note
    Requires the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
    (v0.220+) installed and authenticated (`databricks auth login`) locally —
    these commands are not run as part of this documentation build.

```bash
# Validate the bundle configuration
databricks bundle validate --strict --profile <PROFILE>

# Deploy to the default "dev" target (builds the wheel, uploads resources)
databricks bundle deploy -t dev --profile <PROFILE>

# Run the job end-to-end
databricks bundle run excel_ingestion_job -t dev --profile <PROFILE>

# Deploy to production
databricks bundle deploy -t prod --profile <PROFILE>
```

The job's schedule is defined but `pause_status: PAUSED` by default — unpause
it in `resources/excel_ingestion_job.job.yml` once you're ready for it to run
nightly.
