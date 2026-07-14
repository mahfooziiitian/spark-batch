---
mode: ask
description: Add a new Silver-layer data quality rule to the medallion pipeline
---

# Add a Data Quality Rule (Silver Layer)

I need a new data quality / validation rule added to the Silver layer of `src/07_medallion_prophet_pipeline.py`.

**Column to validate:** [FILL IN — e.g. `revenue`, `store_id`]
**Rule type:** [FILL IN — choose one]

Rule types:
- **Not null** — reject rows where column is NULL
- **Range check** — reject rows outside `[min, max]`
- **Referential integrity** — reject rows whose value is not in a reference list
- **Regex pattern** — reject rows that don't match a pattern (e.g. store ID format)
- **Duplicate** — reject later duplicates, keep most-recent by `ingested_at`
- **Freshness** — warn if no data newer than N days

Please generate:
1. The Spark filter expression for `valid_sdf`
2. The corresponding `rejection_reason` label for the `rejected_sdf` WHEN clause
3. Any new columns or joins needed (e.g. broadcast of reference table)

Quarantine output path is already `REJECT_PATH`. Follow the pattern in `src/07_medallion_prophet_pipeline.py` (Stage 2).
