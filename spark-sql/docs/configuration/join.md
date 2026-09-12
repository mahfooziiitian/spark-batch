# :material-table-merge-cells: Join Configuration

Join configuration settings control which join strategy Spark chooses, when it
broadcasts a table, and how sort-merge joins behave. Correct tuning can eliminate
expensive shuffles entirely.

### :material-animation-play: Interactive Visualization — Join Strategy Picker

Move the dimension-table size slider and change the planner policy to see when Spark can
broadcast, when it falls back to sort-merge, and when disabling sort-merge preference can
make shuffled-hash joins viable for a much smaller build side.

<div id="viz-config-join" class="ts-viz"></div>

<script src="../assets/js/configuration-viz.js"></script>

______________________________________________________________________

## :material-code-tags: Key Settings

| Setting                                | Default | Description                                                  |
| -------------------------------------- | ------- | ------------------------------------------------------------ |
| `spark.sql.autoBroadcastJoinThreshold` | `10MB`  | Tables smaller than this are auto-broadcast                  |
| `spark.sql.join.preferSortMergeJoin`   | `true`  | Prefer sort-merge over shuffled-hash join                    |
| `spark.sql.shuffledHashJoinFactor`     | `3`     | Use shuffled-hash when build side is `N×` smaller than probe |
| `spark.sql.broadcastTimeout`           | `300s`  | Seconds to wait for broadcast to complete                    |

______________________________________________________________________

## :material-information-outline: Behavior

1. When a table's estimated size is below `autoBroadcastJoinThreshold`, Spark collects it to the driver and broadcasts it to every executor — **no shuffle** on that side.
2. Raising the threshold allows larger tables to be broadcast, but increases driver and executor memory pressure.
3. Setting the threshold to `-1` **disables** automatic broadcasting; only explicit `/*+ BROADCAST(t) */` hints can trigger it.
4. `broadcastTimeout` guards against slow or blocked broadcast exchange creation; if collection takes too long, the query fails.
5. `preferSortMergeJoin = false` allows Spark to choose shuffled-hash join when it estimates the build side remains much smaller than the probe side.
6. Join strategy is only as good as the available statistics. Stale or missing stats can keep an otherwise broadcastable table on a shuffle path.

______________________________________________________________________

## :material-chart-timeline-variant: Keep Statistics Fresh

Broadcast eligibility is based on the planner's **estimated** post-filter size, not just
the raw table size on disk. If a dimension should broadcast but `EXPLAIN` still shows a
merge join, refresh stats before forcing hints.

```sql
ANALYZE TABLE dim_product COMPUTE STATISTICS;
ANALYZE TABLE dim_product COMPUTE STATISTICS FOR COLUMNS product_id, category;
```

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Raise broadcast threshold for larger dimensions

```sql
-- Allow dimensions up to 200 MB to be broadcast
SET spark.sql.autoBroadcastJoinThreshold = 209715200;  -- 200 MB

SELECT f.sale_id, d.category
FROM fact_sales AS f
JOIN dim_product AS d ON f.product_id = d.product_id;

RESET spark.sql.autoBroadcastJoinThreshold;
```

### Disable auto-broadcast (force shuffle join)

```sql
-- Useful when broadcast causes driver OOM
SET spark.sql.autoBroadcastJoinThreshold = -1;

SELECT o.order_id, c.name
FROM orders AS o
JOIN customers AS c ON o.customer_id = c.customer_id;

RESET spark.sql.autoBroadcastJoinThreshold;
```

### Force broadcast with a hint

```sql
-- Threshold is -1 but hint still works
SET spark.sql.autoBroadcastJoinThreshold = -1;

SELECT /*+ BROADCAST(dim_store) */
    f.sale_id,
    s.region
FROM fact_sales AS f
JOIN dim_store AS s ON f.store_id = s.store_id;
```

### Force sort-merge join

```sql
SELECT /*+ MERGE(orders, returns) */
    o.order_id,
    r.return_reason
FROM orders AS o
JOIN returns AS r ON o.order_id = r.order_id;
```

### Force shuffled-hash join

```sql
SELECT /*+ SHUFFLE_HASH(orders, small_lookup) */
    o.order_id,
    l.label
FROM orders AS o
JOIN small_lookup AS l ON o.status_code = l.code;
```

### Increase broadcast timeout for slow network or large table

```sql
SET spark.sql.broadcastTimeout = 600;

SELECT /*+ BROADCAST(dim_customer) */
    f.sale_id,
    c.name
FROM fact_sales AS f
JOIN dim_customer AS c ON f.customer_id = c.customer_id;

RESET spark.sql.broadcastTimeout;
```

### Prefer shuffled-hash join over sort-merge

```sql
SET spark.sql.join.preferSortMergeJoin = false;

SELECT o.order_id, l.description
FROM orders AS o
JOIN order_status_lookup AS l ON o.status = l.status_code;

RESET spark.sql.join.preferSortMergeJoin;
```

### Verify join strategy with EXPLAIN

```sql
SET spark.sql.autoBroadcastJoinThreshold = 209715200;

EXPLAIN
SELECT f.sale_id, d.category
FROM fact_sales AS f
JOIN dim_product AS d ON f.product_id = d.product_id;
-- Look for: BroadcastHashJoin vs SortMergeJoin vs ShuffledHashJoin
```

______________________________________________________________________

## :material-lightbulb-outline: When to Tune Join Config

| Scenario                                      | Setting                                             |
| --------------------------------------------- | --------------------------------------------------- |
| Dimension is 20–200 MB and should broadcast   | Raise `autoBroadcastJoinThreshold`                  |
| Driver OOM during broadcast                   | Lower threshold or set it to `-1`                   |
| Broadcast exchange times out                  | Raise `broadcastTimeout`                            |
| Need fine-grained per-query control           | Use `/*+ BROADCAST / MERGE / SHUFFLE_HASH */` hints |
| AQE already handles the post-filter size well | Leave defaults and inspect the final plan           |
