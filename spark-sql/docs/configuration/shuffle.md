# :material-shuffle-variant: Shuffle & Partitioning Config

Shuffle settings control how many partitions are created during aggregations and joins,
and how `INSERT OVERWRITE` interacts with existing partitions.

### :material-animation-play: Interactive Visualization — Partition Count vs Task Size

Slide the configured shuffle partition count up and down, then compare the result with and
without AQE coalescing. The model uses the same 25 GB example discussed below to show when
tasks become too tiny, too heavy, or are merged back toward a better runtime target.

<div id="viz-config-shuffle" class="ts-viz"></div>

<script src="../assets/js/configuration-viz.js"></script>

______________________________________________________________________

## :material-code-tags: Key Settings

| Setting                                                     | Default  | Description                                                                              |
| ----------------------------------------------------------- | -------- | ---------------------------------------------------------------------------------------- |
| `spark.sql.shuffle.partitions`                              | `200`    | Number of partitions after a shuffle (GROUP BY, JOIN)                                    |
| `spark.sql.sources.partitionOverwriteMode`                  | `STATIC` | `STATIC` resolves overwrite targets up front; `DYNAMIC` replaces only touched partitions |
| `spark.sql.adaptive.coalescePartitions.enabled`             | `true`   | AQE merges small shuffle partitions automatically                                        |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize`    | `1MB`    | Minimum target partition size after coalescing                                           |
| `spark.sql.adaptive.coalescePartitions.initialPartitionNum` | —        | Starting partition count before AQE coalesces                                            |

______________________________________________________________________

## :material-information-outline: Behavior

1. The default `200` shuffle partitions is too many for small datasets and too few for very large ones.
2. With AQE enabled, `coalescePartitions.enabled = true` can merge small shuffle partitions at runtime, but AQE cannot create **more** partitions than the value you configured up front.
3. `spark.sql.sources.partitionOverwriteMode = STATIC` resolves the overwrite target from the table and optional partition spec before the write. If no narrower partition spec is provided, `INSERT OVERWRITE` behaves like a full replace.
4. `DYNAMIC` overwrite mode replaces only the partitions produced by the incoming data, which is safer for daily or hourly partition reloads.
5. For partitioned tables, make the overwrite mode explicit when you move SQL between engines or storage layers so the write semantics are never ambiguous.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Tune shuffle partitions for dataset size

```sql
SET spark.sql.shuffle.partitions = 20;

SELECT region, SUM(amount) AS total
FROM daily_sales
GROUP BY region;

RESET spark.sql.shuffle.partitions;
```

### Large dataset: scale partitions to data size

```sql
-- Rule of thumb: target ~128 MB per partition
-- If shuffled data = 25 GB -> 25600 MB / 128 MB ~= 200 partitions
-- If shuffled data = 1 TB  -> 1048576 MB / 128 MB ~= 8192 partitions
SET spark.sql.shuffle.partitions = 8192;

SELECT customer_id, SUM(amount)
FROM orders
GROUP BY customer_id;
```

### Let AQE manage shuffle partitions

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.shuffle.partitions = 1000;
SET spark.sql.adaptive.coalescePartitions.minPartitionSize = 67108864;  -- 64 MB

SELECT product_id, COUNT(*) AS order_count
FROM order_lines
GROUP BY product_id;
-- AQE can coalesce 1000 partitions downward when many are tiny
```

### Dynamic partition overwrite (safe daily reload)

```sql
SET spark.sql.sources.partitionOverwriteMode = DYNAMIC;

INSERT OVERWRITE TABLE sales
SELECT order_id, customer_id, amount, region, order_date
FROM staging_sales
WHERE order_date = CURRENT_DATE();

RESET spark.sql.sources.partitionOverwriteMode;
```

### Static overwrite — intentional full-table replace

```sql
SET spark.sql.sources.partitionOverwriteMode = STATIC;

INSERT OVERWRITE TABLE dim_date
SELECT * FROM new_dim_date;
```

### Verify current partition count at runtime

```sql
SET spark.sql.shuffle.partitions;
```

______________________________________________________________________

## :material-lightbulb-outline: When to Tune

| Scenario                               | Setting                                                    |
| -------------------------------------- | ---------------------------------------------------------- |
| Small dataset, too many tasks          | Lower `shuffle.partitions` to roughly `4 × executor_count` |
| Large dataset, tasks spilling          | Raise `shuffle.partitions` or enable AQE                   |
| AQE enabled with many small partitions | Set a generous starting value and let AQE coalesce         |
| Daily partition reload (idempotent)    | `partitionOverwriteMode = DYNAMIC`                         |
| Intentional full-table replace         | `partitionOverwriteMode = STATIC`                          |
