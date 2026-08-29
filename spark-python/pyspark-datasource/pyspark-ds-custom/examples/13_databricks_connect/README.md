# Example 13 — Databricks Connect

Run PySpark custom data source examples **from your local IDE** against a
remote Databricks cluster using
[Databricks Connect](https://docs.databricks.com/en/dev-tools/databricks-connect/).

## Prerequisites

1. **Install the optional dependency:**

    ```bash
    uv sync --extra databricks
    ```

2. **Configure Databricks Connect** — set environment variables or a profile:

    ```bash
    export DATABRICKS_HOST="https://<workspace>.cloud.databricks.com"
    export DATABRICKS_CLUSTER_ID="<cluster-id>"
    export DATABRICKS_TOKEN="<pat-token>"
    ```

    Or configure via `~/.databrickscfg` profile and set `DATABRICKS_PROFILE`.

3. **Cluster requirements:**
    - DBR 16.4 LTS+ (PySpark 4 / Python Data Source API GA)
    - The `custom_ds` wheel installed on the cluster (`pyspark-ds-custom`)

## Scripts

| Script | Description |
|--------|-------------|
| `dbconnect_batch_read.py` | Batch read from a REST API via Databricks Connect |
| `dbconnect_batch_write.py` | Batch write (POST) to a REST API via Databricks Connect |
| `dbconnect_sql.py` | SQL queries over REST API data via Databricks Connect |

## How It Works

Databricks Connect replaces the local Spark engine with a thin client that
sends operations to a remote cluster. The `create_dbconnect_session()` helper
in these examples builds the remote `SparkSession` — the rest of the code
(register, read, write, SQL) is **identical** to the local examples.

```
┌──────────────┐         gRPC / SC         ┌────────────────────┐
│  Local IDE   │ ◄──────────────────────► │  Databricks Cluster │
│  (thin client)│                          │  (Spark engine)     │
│              │   .read.format("restapi") │  → HTTP → REST API  │
└──────────────┘                           └────────────────────┘
```
