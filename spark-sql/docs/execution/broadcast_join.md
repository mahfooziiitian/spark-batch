# :material-broadcast: Broadcast Join

A broadcast join avoids a shuffle by sending a copy of the smaller table to every executor.
It is the fastest join strategy when one side fits in memory but is harmful when misapplied
to large tables.

---

## :material-information-outline: Behavior

1. Spark automatically chooses broadcast join when the smaller table's estimated size is
   below `spark.sql.autoBroadcastJoinThreshold` (default **10 MB**).
2. The smaller ("broadcast") side is collected to the driver, serialized, and pushed to
   every executor — no shuffle occurs on that side.
3. The larger ("streamed") side is read partition-by-partition on each executor; each
   partition is probed against the in-memory hash table built from the broadcast side.
4. Broadcast join supports `=` equi-joins only. Non-equi joins (`<`, `>`, `!=`) fall
   back to `BroadcastNestedLoopJoin` — O(n × m) — which is extremely slow on large tables.
5. If the optimizer's size estimate is wrong (e.g., no statistics), it may choose
   `SortMergeJoin` even when broadcast would be faster. Use the `BROADCAST` hint to force it.
6. Setting `spark.sql.autoBroadcastJoinThreshold = -1` disables automatic broadcasting;
   only explicit hints will trigger it.

---

## :material-code-tags: Syntax

```sql
-- Hint: broadcast the smaller table
SELECT /*+ BROADCAST(dim) */ f.order_id, d.category
FROM fact_orders AS f
JOIN dim_product AS d
    ON f.product_id = d.product_id;

-- Alternative hint name
SELECT /*+ BROADCASTJOIN(dim) */ ...
SELECT /*+ MAPJOIN(dim) */       ...    -- Hive-compatible alias

-- Disable auto-broadcast for this query only
SET spark.sql.autoBroadcastJoinThreshold = -1;
```

---

## :material-flask-outline: Practical Examples

### Star-schema fact-dimension join

```sql
-- dim_product is ~5 MB — fits comfortably in memory
SELECT /*+ BROADCAST(dim_product) */
    f.order_id,
    f.amount,
    p.category,
    p.brand
FROM fact_orders AS f
JOIN dim_product AS p
    ON f.product_id = p.product_id;
```

`EXPLAIN` confirms:

```
BroadcastHashJoin [product_id#1], [product_id#5], Inner, BuildRight
:- FileScan parquet fact_orders [order_id#0, amount#2, product_id#1]
+- BroadcastExchange HashedRelationBroadcastMode(List(product_id#5))
   +- FileScan parquet dim_product [product_id#5, category#6, brand#7]
```

- `BuildRight` — the right side (dim_product) was broadcast.
- No `Exchange` on the left side — **no shuffle for fact_orders**.

### Multiple small dimensions

```sql
SELECT /*+ BROADCAST(d_product, d_store, d_date) */
    f.sale_id,
    p.category,
    s.region,
    d.fiscal_quarter,
    SUM(f.revenue) AS total_revenue
FROM fact_sales AS f
JOIN dim_product AS p ON f.product_id = p.product_id
JOIN dim_store   AS s ON f.store_id   = s.store_id
JOIN dim_date    AS d ON f.sale_date  = d.date_key
GROUP BY p.category, s.region, d.fiscal_quarter;
```

### Increase threshold for a large-ish dimension

```sql
-- Temporarily raise threshold to 100 MB for this session
SET spark.sql.autoBroadcastJoinThreshold = 104857600;

SELECT f.*, d.description
FROM fact_events AS f
JOIN dim_event_type AS d ON f.event_type_id = d.event_type_id;

-- Reset to default
SET spark.sql.autoBroadcastJoinThreshold = 10485760;
```

### Force sort-merge join (disable broadcast)

```sql
SELECT /*+ MERGE(orders, returns) */
    o.order_id,
    r.return_reason
FROM orders AS o
JOIN returns AS r ON o.order_id = r.order_id;
```

---

## :material-swap-horizontal: Join Strategy Comparison

| Strategy | Trigger | Shuffle | Memory | Best for |
|----------|---------|---------|--------|---------|
| `BroadcastHashJoin` | Size < threshold or `BROADCAST` hint | One side only (driver → executors) | Broadcast table must fit in executor memory | Small dimension tables |
| `ShuffledHashJoin` | Medium table; hash fits in memory | Both sides | Build side hashed per partition | Medium tables, skewed data |
| `SortMergeJoin` | Default for large tables | Both sides (sort + merge) | Low — streams sorted partitions | Large-to-large joins |
| `BroadcastNestedLoopJoin` | Non-equi join | One side (if possible) | High — nested loop O(n×m) | Last resort only |

---

## :material-shield-outline: Pitfalls

| Pitfall | Symptom | Fix |
|---------|---------|-----|
| Broadcasting a large table | Driver OOM or executor OOM | Lower `autoBroadcastJoinThreshold` or remove hint |
| Stale statistics cause missed broadcast | `SortMergeJoin` chosen for a small table | Run `ANALYZE TABLE`; or add `BROADCAST` hint |
| Non-equi condition with broadcast | `BroadcastNestedLoopJoin` (very slow) | Rewrite as equi-join + post-filter |
| Broadcast on `NULL` join key | Rows silently dropped | Use `<=>` or filter NULLs before join |

---

## :material-lightbulb-outline: When to Use

| Scenario | Recommendation |
|----------|---------------|
| Dimension table < 10 MB | Automatic — no hint needed |
| Dimension table 10 MB – 200 MB | Add `/*+ BROADCAST(dim) */` hint |
| Dimension table > 200 MB | Use `SortMergeJoin`; consider pre-aggregating |
| Star schema with many small dimensions | Hint all dimension tables |
| Query planner chooses wrong strategy | Use `EXPLAIN` to verify, then apply hint |

!!! warning "Broadcast and driver memory"
    The broadcast table is first **collected to the driver** before being pushed to executors.
    A 200 MB broadcast table means 200 MB on the driver **plus** 200 MB on every executor.
    Monitor driver heap usage when raising `autoBroadcastJoinThreshold`.
