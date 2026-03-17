# Sales ETL Pipeline

A realistic multi-step ETL job that demonstrates the patterns used in production
data engineering: ingest, validate, enrich via join, aggregate KPIs, and write
partitioned Parquet output.

## Pipeline overview

```
Raw orders (CSV / in-memory)
        │
        ▼
   1. Ingest ──────────────────── schema enforcement
        │
        ▼
   2. Clean ──────────────────── drop nulls, filter bad rows
        │
        ▼
   3. Enrich ─────────────────── LEFT JOIN with product dimension
        │
        ▼
   4. KPI computation ─────────── net_revenue = qty × price × (1 − discount)
        │
        ▼
   5. Aggregate ───────────────── group by region + category
        │
        ▼
   6. Write Parquet ────────────── partitioned by region
```

## Run it

=== "Local (direct)"
    ```bash
    python spark-submit/sales_analysis.py
    ```

=== "spark-submit local"
    ```bash
    spark-submit --master local[*] spark-submit/sales_analysis.py
    ```

=== "YARN"
    ```bash
    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      --num-executors 4 \
      --executor-memory 4g \
      spark-submit/sales_analysis.py
    ```

=== "Kubernetes"
    ```bash
    spark-submit \
      --master k8s://https://<api-server>:6443 \
      --deploy-mode cluster \
      --conf spark.kubernetes.container.image=my-registry/pyspark:3.5 \
      local:///opt/spark/work-dir/sales_analysis.py
    ```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `INPUT_PATH` | in-memory sample | Path / URI to raw orders data |
| `DIM_PATH` | in-memory sample | Path / URI to product dimension table |
| `OUTPUT_PATH` | `/tmp/sales_etl_output` | Destination for Parquet output |

## Output schema

| Column | Type | Description |
|--------|------|-------------|
| `region` | string | Sales region (partition key) |
| `category` | string | Product category |
| `num_orders` | long | Order count |
| `total_units` | long | Units sold |
| `total_revenue` | double | Net revenue after discounts |
| `avg_order_revenue` | double | Average revenue per order |

## Data quality rules applied

!!! info "Rows dropped during cleaning"
    | Rule | Reason |
    |------|--------|
    | `region IS NOT NULL` | Aggregation by region is meaningless without it |
    | `quantity > 0` | Negative quantities indicate bad source data |
    | `unit_price > 0` | Zero-price rows skew revenue calculations |

## Source

```python title="spark-submit/sales_analysis.py"
--8<-- "spark-submit/sales_analysis.py"
```
