# :material-lightning-bolt: Adaptive Query Execution (AQE) Config

Adaptive Query Execution re-optimizes a running query at shuffle boundaries using
actual runtime statistics. It is enabled by default in Spark 3.x and handles skew,
partition sizing, and join strategy changes automatically.

---

## :material-code-tags: Key Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `spark.sql.adaptive.enabled` | `true` | Master switch for AQE |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Merge small shuffle partitions |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | `1MB` | Minimum size after coalescing |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64MB` | Target partition size for coalescing |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Split skewed partitions in sort-merge join |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5` | A partition is skewed if `size > factor × median` |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `256MB` | Minimum absolute size to consider skewed |
| `spark.sql.adaptive.localShuffleReader.enabled` | `true` | Use local shuffle reader when possible |
| `spark.sql.optimizer.dynamicPartitionPruning.enabled` | `true` | Push runtime filter to partition scan |

---

## :material-information-outline: Behavior

1. AQE collects **actual** row counts, partition sizes, and skew statistics at each shuffle stage and uses them to re-plan the next stage.
2. **Coalesce partitions**: small shuffle output partitions are merged before the next stage — reduces task overhead without requiring upfront tuning.
3. **Skew join handling**: a skewed partition (much larger than the median) is automatically split into sub-partitions; the matching side is replicated to each sub-partition.
4. **Dynamic join strategy switching**: if AQE discovers one side of a sort-merge join is small enough after filtering, it switches to a broadcast join at runtime.
5. **Dynamic partition pruning (DPP)**: applies the result of a dimension filter as a runtime filter on the fact table partition scan — equivalent to an in-clause partition prune.

---

## :material-flask-outline: Practical Examples

### Enable AQE with recommended settings

```sql
SET spark.sql.adaptive.enabled                                      = true;
SET spark.sql.adaptive.coalescePartitions.enabled                   = true;
SET spark.sql.adaptive.advisoryPartitionSizeInBytes                 = 67108864;  -- 64 MB
SET spark.sql.adaptive.skewJoin.enabled                             = true;
SET spark.sql.adaptive.skewJoin.skewedPartitionFactor               = 5;
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes     = 268435456; -- 256 MB
SET spark.sql.optimizer.dynamicPartitionPruning.enabled             = true;
```

### Disable AQE for a specific query (debugging)

```sql
SET spark.sql.adaptive.enabled = false;

-- Run query without AQE to observe the static plan
EXPLAIN
SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id;

RESET spark.sql.adaptive.enabled;
```

### Tune skew thresholds for a known skewed column

```sql
-- customer_id has one customer with 40 % of all rows
SET spark.sql.adaptive.skewJoin.skewedPartitionFactor           = 3;   -- more sensitive
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 134217728;  -- 128 MB

SELECT c.name, SUM(o.amount) AS ltv
FROM orders AS o
JOIN customers AS c ON o.customer_id = c.customer_id
GROUP BY c.name;
```

### Dynamic partition pruning on a star-schema query

```sql
-- DPP enabled: the filter on dim_date is pushed to the fact table scan
-- Only partitions matching the date filter are opened
SET spark.sql.optimizer.dynamicPartitionPruning.enabled = true;

SELECT
    f.sale_id,
    f.revenue,
    d.fiscal_quarter
FROM fact_sales AS f
JOIN dim_date AS d ON f.sale_date = d.date_key
WHERE d.fiscal_year = 2024 AND d.fiscal_quarter = 'Q1';
-- Spark pushes fiscal_year=2024 / Q1 as a partition filter on fact_sales
```

### Verify AQE is taking effect

```sql
-- Run with AQE and inspect the final plan (post-execution)
-- In Databricks: check the Stage Details → Shuffle Read/Write sizes
-- In SQL: EXPLAIN after execution shows the adaptive plan
EXPLAIN FORMATTED
SELECT region, COUNT(*) FROM orders GROUP BY region;
-- Look for: AdaptiveSparkPlan, CustomShuffleReader, AQEShuffleRead
```

### Force AQE to reconsider a join strategy

```sql
-- AQE will switch SortMergeJoin → BroadcastHashJoin if one side shrinks enough
-- after a selective filter — no hint needed
SELECT /*+ MERGE(orders, customers) */
    o.order_id, c.name
FROM orders AS o
JOIN customers AS c ON o.customer_id = c.customer_id
WHERE o.order_date = CURRENT_DATE();
-- If the filtered orders side is < autoBroadcastJoinThreshold, AQE switches strategy
```

---

## :material-lightbulb-outline: When to Tune AQE Settings

| Scenario | Setting to change |
|----------|-----------------|
| Many tiny shuffle tasks | Lower `advisoryPartitionSizeInBytes` to merge more |
| Skew join causing one slow task | Lower `skewedPartitionFactor` or `skewedPartitionThresholdInBytes` |
| DPP not firing on star-schema query | Verify `dynamicPartitionPruning.enabled = true` |
| Debugging — want static plan | `SET adaptive.enabled = false` temporarily |
| AQE changing join strategy unexpectedly | Inspect with `EXPLAIN FORMATTED`; add join hint if needed |
