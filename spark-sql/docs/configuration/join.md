# :material-table-merge-cells: Join Configuration

Join configuration settings control which join strategy Spark chooses, when it
broadcasts a table, and how sort-merge joins behave. Correct tuning can eliminate
expensive shuffles entirely.

---

## :material-code-tags: Key Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `spark.sql.autoBroadcastJoinThreshold` | `10MB` | Tables smaller than this are auto-broadcast |
| `spark.sql.join.preferSortMergeJoin` | `true` | Prefer sort-merge over shuffled-hash join |
| `spark.sql.shuffledHashJoinFactor` | `3` | Use shuffled-hash when build side is `N×` smaller than probe |
| `spark.sql.broadcastTimeout` | `300s` | Seconds to wait for broadcast to complete |

---

## :material-information-outline: Behavior

1. When a table's estimated size is below `autoBroadcastJoinThreshold`, Spark collects it to the driver and broadcasts it to every executor — **no shuffle** on that side.
2. Raising the threshold allows larger tables to be broadcast, but increases driver and executor memory pressure.
3. Setting the threshold to `-1` **disables** automatic broadcasting; only explicit `/*+ BROADCAST(t) */` hints will trigger it.
4. `broadcastTimeout` guards against driver OOM stalls — if the broadcast collection takes longer than this, the job fails with a timeout error.
5. `preferSortMergeJoin = false` allows Spark to choose shuffled-hash join when it estimates the build side fits in memory per partition.

---

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
    f.sale_id, s.region
FROM fact_sales AS f
JOIN dim_store AS s ON f.store_id = s.store_id;
```

### Force sort-merge join

```sql
SELECT /*+ MERGE(orders, returns) */
    o.order_id, r.return_reason
FROM orders AS o
JOIN returns AS r ON o.order_id = r.order_id;
```

### Force shuffled-hash join

```sql
SELECT /*+ SHUFFLE_HASH(orders, small_lookup) */
    o.order_id, l.label
FROM orders AS o
JOIN small_lookup AS l ON o.status_code = l.code;
```

### Increase broadcast timeout for slow network/large table

```sql
-- Allow up to 10 minutes for broadcast collection
SET spark.sql.broadcastTimeout = 600;

SELECT /*+ BROADCAST(dim_customer) */
    f.sale_id, c.name
FROM fact_sales AS f
JOIN dim_customer AS c ON f.customer_id = c.customer_id;

RESET spark.sql.broadcastTimeout;
```

### Prefer shuffled-hash join over sort-merge

```sql
-- Avoid the sort step when build side is much smaller than probe side
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
-- Look for: BroadcastHashJoin (broadcast) vs SortMergeJoin (shuffle both sides)
```

---

## :material-lightbulb-outline: When to Tune Join Config

| Scenario | Setting |
|----------|---------|
| Dimension is 20–200 MB — should broadcast but doesn't | Raise `autoBroadcastJoinThreshold` |
| Driver OOM during broadcast | Lower threshold or set to `-1` |
| Broadcast collection times out | Raise `broadcastTimeout` |
| Need fine-grained per-query control | Use `/*+ BROADCAST / MERGE / SHUFFLE_HASH */` hints |
| AQE already handles it | Leave defaults; AQE will switch strategy at runtime |
