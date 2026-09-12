# :material-scale-unbalanced: Skewed Join Overview

Join skew happens when one or a few key values send far more rows to one shuffle partition than the others, creating long tail tasks even when the overall cluster is idle.

### :material-animation-play: Interactive Visualization — Skew Handling Options

<div id="viz-joins-skew-overview" class="ts-viz"></div>

Switch among the main skew remedies to compare what Spark automates for you and what requires manual data reshaping.

<script src="../../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-check-decagram: What Was Verified

- Open-source PySpark 4.2 recognizes the AQE skew settings `spark.sql.adaptive.skewJoin.enabled`, `spark.sql.adaptive.skewJoin.skewedPartitionFactor`, and `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`. Visible partition splitting still depends on runtime shuffle sizes, so verify the executed plan on production-shaped data.
- The SQL hint forms shown in many Databricks-oriented articles, such as `/*+ SKEW(...) */`, did not produce a distinct skew-aware operator in local open-source Spark 4.2 checks; the test query simply planned its normal join.
- Broadcast, bucketing, manual key separation, and salting are all still relevant because AQE only helps after a shuffle-based plan exists.

______________________________________________________________________

## :material-cog-outline: AQE First

For ordinary equi-joins on large tables, start with AQE:

```sql
set spark.sql.adaptive.enabled = true;
set spark.sql.adaptive.skewJoin.enabled = true;
set spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5;
set spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 268435456;
```

Interpretation:

- `skewedPartitionFactor` compares a partition with the median shuffled partition.
- `skewedPartitionThresholdInBytes` prevents tiny toy partitions from being treated as skew just because the median is even smaller.

______________________________________________________________________

## :material-table: Choose the Technique by Situation

| Situation                                                       | Better first response              |
| --------------------------------------------------------------- | ---------------------------------- |
| Small dimension, large skewed fact                              | Broadcast the dimension            |
| Large shuffle join with a few hot partitions                    | AQE skew handling                  |
| Repeated joins against the same expensive filtered intermediate | Cache after filtering              |
| Known hot keys on one side                                      | Split hot keys into a special path |
| Extreme single-key skew that AQE cannot smooth enough           | Salt the join key                  |
| Reused bucketed datasets with the same join key                 | Bucketing to avoid shuffle         |

______________________________________________________________________

## :material-alert-outline: Important Scope Note

!!! warning "Open-source Spark vs vendor-specific syntax"

    Do not assume every `SKEW(...)` hint example you find online applies to open-source Spark SQL 4.2. Verify the actual physical plan in your runtime.

______________________________________________________________________

## :material-lightbulb-outline: Practical Rule

If the skew only exists because a small lookup table is joined to a massive fact table, broadcasting that lookup table is usually simpler and safer than introducing salting immediately.
