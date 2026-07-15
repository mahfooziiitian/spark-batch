# :material-shuffle-variant: Shuffle & Partitioning Config

Shuffle settings control how many partitions are created during aggregations and joins,
and how `INSERT OVERWRITE` interacts with existing partitions.

---

## :material-code-tags: Key Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `spark.sql.shuffle.partitions` | `200` | Number of partitions after a shuffle (GROUP BY, JOIN) |
| `spark.sql.sources.partitionOverwriteMode` | `STATIC` | `STATIC` replaces whole table; `DYNAMIC` replaces only touched partitions |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | AQE merges small shuffle partitions automatically |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | `1MB` | Minimum target partition size after coalescing |
| `spark.sql.adaptive.coalescePartitions.initialPartitionNum` | — | Starting partition count before AQE coalesces |

---

## :material-information-outline: Behavior

1. The default `200` shuffle partitions is too many for small datasets (causes many tiny tasks) and too few for very large ones (causes large tasks and spill).
2. With **AQE enabled**, `coalescePartitions.enabled = true` automatically merges small shuffle partitions at runtime — set `shuffle.partitions` to a large value and let AQE shrink it.
3. `STATIC` overwrite mode (default for non-Delta): `INSERT OVERWRITE` replaces the **entire table**. `DYNAMIC`: replaces only the partitions present in the new data.
4. Delta tables default to `DYNAMIC` overwrite mode regardless of this setting.

---

## :material-flask-outline: Practical Examples

### Tune shuffle partitions for dataset size

```sql
-- Small dataset: reduce from 200 to avoid tiny tasks
SET spark.sql.shuffle.partitions = 20;

SELECT region, SUM(amount) AS total
FROM daily_sales
GROUP BY region;

RESET spark.sql.shuffle.partitions;
```

### Large dataset: scale partitions to data size

```sql
-- Rule of thumb: target ~128 MB per partition
-- If shuffled data = 25 GB → 25600 MB / 128 MB ≈ 200 partitions (fine)
-- If shuffled data = 1 TB  → 1048576 MB / 128 MB ≈ 8192 partitions
SET spark.sql.shuffle.partitions = 8192;

SELECT customer_id, SUM(amount)
FROM orders
GROUP BY customer_id;
```

### Let AQE manage shuffle partitions

```sql
-- Set high; AQE coalesces down at runtime
SET spark.sql.adaptive.enabled                              = true;
SET spark.sql.adaptive.coalescePartitions.enabled           = true;
SET spark.sql.shuffle.partitions                            = 1000;
SET spark.sql.adaptive.coalescePartitions.minPartitionSize  = 67108864;  -- 64 MB

SELECT product_id, COUNT(*) AS order_count
FROM order_lines
GROUP BY product_id;
-- AQE will coalesce 1000 partitions to the appropriate number at runtime
```

### Dynamic partition overwrite (safe daily reload)

```sql
SET spark.sql.sources.partitionOverwriteMode = DYNAMIC;

-- Only the partitions touched by the new data are replaced
INSERT OVERWRITE TABLE sales
SELECT order_id, customer_id, amount, region, order_date
FROM staging_sales
WHERE order_date = CURRENT_DATE();

RESET spark.sql.sources.partitionOverwriteMode;
```

### Static overwrite — intentional full-table replace

```sql
SET spark.sql.sources.partitionOverwriteMode = STATIC;

-- Replaces the ENTIRE table
INSERT OVERWRITE TABLE dim_date
SELECT * FROM new_dim_date;
```

### Verify current partition count at runtime

```sql
-- After a shuffle-heavy query, inspect partition stats
SET spark.sql.shuffle.partitions;
-- Returns current value for this session
```

---

## :material-lightbulb-outline: When to Tune

| Scenario | Setting |
|----------|---------|
| Small dataset, too many tasks | Lower `shuffle.partitions` to `4 × executor_count` |
| Large dataset, tasks spilling | Raise `shuffle.partitions` or enable AQE |
| AQE enabled | Set `shuffle.partitions = 1000` and let AQE coalesce |
| Daily partition reload (idempotent) | `partitionOverwriteMode = DYNAMIC` |
| Full table replace | `partitionOverwriteMode = STATIC` |
