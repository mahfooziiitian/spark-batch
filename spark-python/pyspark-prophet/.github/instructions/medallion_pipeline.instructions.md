---
applyTo: "src/07_*.py"
---

# Medallion / Lakehouse Pipeline Patterns

## Layer Responsibilities
| Layer      | Rule |
|------------|------|
| **Bronze** | Raw bytes only. No transformations. Partition by `ingest_date`. |
| **Silver** | Validate, deduplicate, gap-fill, enrich. Partition by group key. |
| **Gold**   | Forecasts, KPIs, anomaly flags. Partition by `run_date` + group key. |

## Schema Validation (Bronze → Silver)
Cast strings to types first, then apply filter rules:
```python
typed = raw.withColumn("ds", F.to_date("sale_date", "yyyy-MM-dd")) \
           .withColumn("y",  F.col("revenue").cast(DoubleType()))
valid   = typed.filter(F.col("ds").isNotNull() & F.col("y").isNotNull() & (F.col("y") >= 0))
rejected = typed.subtract(valid)
```
Always write rejected rows to a quarantine path with a `rejection_reason` column.

## Deduplication
Use `row_number()` over an `ingested_at`-ordered window — NOT `dropDuplicates()` —
so the most recently ingested version wins:
```python
w = Window.partitionBy("store", "ds").orderBy(F.desc("ingested_at"))
deduped = sdf.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
```

## Gap Filling
Always join to a full date spine before forward-filling:
```python
full_grid = date_spine.join(actuals, on=["group", "ds"], how="left")
filled    = full_grid.withColumn("y", F.last("y", ignorenulls=True).over(w_ff))
```

## Incremental Processing
Eligibility check MUST use full history:
```python
eligible        = silver_full.groupby("store").agg(F.count("ds").alias("n")).filter(F.col("n") >= MIN_DAYS)
stores_new_data = silver_full.filter(F.col("ds") > last_checkpoint).select("store").distinct()
stores_to_run   = eligible.join(stores_new_data, on="store", how="inner")
model_input     = silver_full.join(stores_to_run, on="store", how="inner")  # full history!
```

## Checkpointing
Store the last-processed date as a tiny JSON file, not inside Parquet:
```python
def write_checkpoint(path, last_date):
    os.makedirs(path, exist_ok=True)
    with open(f"{path}/checkpoint.json", "w") as f:
        json.dump({"last_processed": last_date}, f)
```

## Idempotent Writes
All writes use `mode("overwrite")` partitioned by date; re-running the same
`run_date` safely replaces that partition without touching others.

## Pipeline Configuration
Keep all tunable values in a single `PIPELINE_CONFIG` dict at the top of the
file — no hard-coded constants scattered through business logic.
