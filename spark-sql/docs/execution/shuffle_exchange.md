# :material-shuffle-variant: Shuffle & Exchange

A shuffle (Exchange) redistributes data across executors by hash-partitioning or sorting it.
Shuffles are the most expensive operation in Spark — they write intermediate data to disk and
transfer it over the network. Minimising shuffle count and size is the single highest-impact
optimisation for most queries.

---

## :material-information-outline: Behavior

1. A shuffle is introduced whenever two stages need data co-located by the same key: `GROUP BY`,
   `JOIN` (non-broadcast), `DISTINCT`, and `ORDER BY` all require shuffles.
2. Spark hashes each row's partition key, writes output to local shuffle files, and then each
   downstream task reads its assigned hash buckets from every upstream executor.
3. The number of shuffle partitions is controlled by `spark.sql.shuffle.partitions`
   (default **200**). With AQE enabled, this is adjusted at runtime.
4. `REPARTITION(n)` forces a new hash shuffle into exactly `n` partitions.
5. `COALESCE(n)` merges partitions locally **without** a shuffle — it narrows the partition
   count but may produce skewed partitions.
6. `REPARTITION(n, col)` hash-partitions by `col` — useful before joins or aggregations on
   that column to pre-shuffle the data.
7. `SORT WITHIN PARTITIONS` sorts rows within each partition without changing the number of
   partitions or shuffling data between executors.

---

## :material-code-tags: Syntax

```sql
-- Control shuffle partition count for the session
SET spark.sql.shuffle.partitions = 50;

-- Repartition into N partitions (hash shuffle)
SELECT /*+ REPARTITION(50) */ * FROM large_table;

-- Repartition by column (co-locate by key, then aggregate/join)
SELECT /*+ REPARTITION(50, customer_id) */ * FROM orders;

-- Coalesce without shuffle (reduce partition count only)
SELECT /*+ COALESCE(10) */ * FROM small_result;

-- Sort within each partition (no cross-executor shuffle)
SELECT /*+ SORT_MERGE_JOIN(orders, returns) */ ...
FROM orders JOIN returns ON orders.order_id = returns.order_id;
```

---

## :material-flask-outline: Practical Examples

### Tune shuffle partitions for dataset size

```sql
-- Default 200 is too many for a small dataset — causes overhead
SET spark.sql.shuffle.partitions = 20;

SELECT region, SUM(amount) AS total
FROM daily_sales
GROUP BY region;

-- Reset to default
SET spark.sql.shuffle.partitions = 200;
```

### Pre-partition before a join

```sql
-- Both tables are large; pre-partitioning by the join key avoids double shuffle
-- if the same key is used in multiple downstream operations
CREATE OR REPLACE TEMP VIEW orders_repartitioned AS
SELECT /*+ REPARTITION(100, customer_id) */ *
FROM orders;

CREATE OR REPLACE TEMP VIEW customers_repartitioned AS
SELECT /*+ REPARTITION(100, customer_id) */ *
FROM customers;

SELECT
    c.name,
    SUM(o.amount) AS lifetime_value
FROM orders_repartitioned AS o
JOIN customers_repartitioned AS c
    ON o.customer_id = c.customer_id
GROUP BY c.name;
```

### Reduce partitions after a filtering step

```sql
-- After filtering to a small subset, coalesce avoids writing many tiny files
INSERT INTO summary_table
SELECT /*+ COALESCE(4) */
    region,
    SUM(amount) AS total
FROM sales
WHERE order_date = CURRENT_DATE()
GROUP BY region;
```

### Identify shuffles with EXPLAIN

```sql
EXPLAIN
SELECT customer_id, COUNT(*) AS order_count
FROM orders
GROUP BY customer_id;
```

```text
*(2) HashAggregate(keys=[customer_id#1], ...)
+- Exchange hashpartitioning(customer_id#1, 200)    ← shuffle here
   +- *(1) HashAggregate(keys=[customer_id#1], ...) ← partial agg (no shuffle)
      +- *(1) FileScan parquet [customer_id#1]
```

### Sort within partitions for file output

```sql
-- Produces sorted files per partition — useful for downstream range queries
-- but does NOT sort across partitions (use ORDER BY for global sort)
SELECT /*+ REPARTITION(10, region) */
    region,
    order_date,
    order_id,
    amount
FROM sales
SORT BY order_date;   -- sort within each partition, no global shuffle
```

### Avoid ORDER BY on large tables (global sort = full shuffle)

```sql
-- ❌ Global ORDER BY on billions of rows — full reshuffle + sort into 1 partition
SELECT * FROM orders ORDER BY order_date;

-- ✅ Use window rank + filter if you only need top N per group
SELECT region, order_id, amount
FROM (
    SELECT
        region,
        order_id,
        amount,
        RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS rnk
    FROM orders
)
WHERE rnk <= 10;
```

---

## :material-swap-horizontal: REPARTITION vs COALESCE

| Aspect | `REPARTITION(n)` | `COALESCE(n)` |
|--------|-----------------|--------------|
| Shuffle | Yes — full hash shuffle | No — local merge only |
| Can increase partitions | Yes | No |
| Partition balance | Balanced (hash distributed) | May produce skewed partitions |
| Use case | Before joins/aggregations on a key | Reduce small result sets before writing |

---

## :material-swap-horizontal: Common Shuffle Sources

| Operation | Exchange type | Notes |
|-----------|--------------|-------|
| `GROUP BY` | `hashpartitioning` | Two-phase: partial agg then shuffle + final agg |
| `JOIN` (SortMergeJoin) | `hashpartitioning` | Both sides shuffled |
| `ORDER BY` | `rangepartitioning` | Global range sort — single output partition if no `LIMIT` |
| `DISTINCT` | `hashpartitioning` | Equivalent to `GROUP BY` all columns |
| `UNION ALL` + distinct | `hashpartitioning` | `UNION` deduplication triggers shuffle |
| `REPARTITION(n, col)` | `hashpartitioning` | Explicit shuffle |
| Window function | `hashpartitioning` | Partition by the `PARTITION BY` key |

---

## :material-lightbulb-outline: When to Tune Shuffles

| Scenario | Recommendation |
|----------|---------------|
| `shuffle.partitions = 200` on a small dataset | Lower to `4 × executor_count` |
| AQE enabled — let Spark auto-tune | Keep default; enable `coalescePartitions.enabled` |
| Many downstream operations share the same join key | Pre-repartition into a cached view |
| Writing a partitioned table — too many small files | `COALESCE` or `REPARTITION` before `INSERT` |
| Shuffle spill to disk (slow query) | Increase `spark.executor.memory` or reduce shuffle partitions |

!!! tip "Let AQE handle shuffle partitions"
    With Adaptive Query Execution (`spark.sql.adaptive.enabled = true`, the default in
    Spark 3.x), Spark automatically coalesces small shuffle partitions at runtime.
    Set `spark.sql.shuffle.partitions` to a large value (e.g. `1000`) and let AQE
    merge them down — no manual tuning needed.
