# :material-cog-transfer: Broadcast Nested Loop Join (BNLJ)

BNLJ is Spark's fallback join strategy for **non-equi joins** and **cross joins** where hash-based strategies cannot be applied.

---

## :material-sitemap: Overview

```mermaid
graph LR
    D[Driver] -->|broadcast small table| E1[Executor 1]
    D -->|broadcast small table| E2[Executor 2]
    E1 -->|nested loop over each row| L1[Large DF Part 1]
    E2 -->|nested loop over each row| L2[Large DF Part 2]
```

---

## :material-cog-outline: When Spark Uses BNLJ

| Trigger | Example |
|---------|---------|
| Non-equi join condition | `ON a.amount > b.min_amount` |
| `CROSS JOIN` (no predicate) | `FROM a CROSS JOIN b` |
| `OR` condition in join | `ON a.id = b.id OR a.code = b.code` |
| `BROADCAST` hint on a non-equi join | `/*+ BROADCAST(dim) */` with `<`, `LIKE`, `!=` |

---

## :material-refresh: How It Works

1. **Broadcast** — The smaller DataFrame is serialized and sent to every executor.
2. **Nested loop** — Each executor iterates over every row of its local partition of the large DataFrame. For each large-table row, it iterates over every row of the broadcast copy and evaluates the join condition.
3. **Emit** — Rows where the condition evaluates to `true` are included in the result.

The time complexity is **O(N × M)** per executor, making this strategy expensive for large inputs.

---

## :material-sitemap: Execution Diagram

```mermaid
flowchart TB
    subgraph Driver
        smallDF[Small DataFrame]
    end

    subgraph WorkerNode1
        smallCopy1[Small DF Copy]
        largePart1[Large DF Partition 1]
    end

    subgraph WorkerNode2
        smallCopy2[Small DF Copy]
        largePart2[Large DF Partition 2]
    end

    smallDF -- Broadcast --> smallCopy1
    smallDF -- Broadcast --> smallCopy2

    smallCopy1 -- Nested Loop --> largePart1
    smallCopy2 -- Nested Loop --> largePart2

    largePart1 --> out1[Join Output 1]
    largePart2 --> out2[Join Output 2]
```

---

## :material-flask-outline: Examples

```sql
-- Non-equi range join (amount must fall within a slab)
SELECT t.transaction_id, t.amount, s.tax_rate
FROM transactions t
JOIN tax_slabs s
    ON t.amount BETWEEN s.min_amount AND s.max_amount;

-- Date overlap join
SELECT a.event_id, b.campaign_id
FROM events a
JOIN campaigns b
    ON a.event_date BETWEEN b.start_date AND b.end_date;

-- CROSS JOIN (all combinations)
SELECT p.product_id, r.region_name
FROM products p
CROSS JOIN regions r;
```

---

## :material-alert-circle: Performance Notes

| Concern | Detail |
|---------|--------|
| Cost | O(N × M) per executor — avoid for large inputs |
| OOM risk | Broadcast side must fit in executor memory |
| No sort required | Neither side needs to be sorted |
| Join types | Supports all except full outer join (when broadcast side is the right table) |

!!! warning
    BNLJ can cause out-of-memory errors and very long run times on large datasets.
    Pre-filter both sides aggressively before a non-equi join.

---

## :material-magnify: Behavior Notes

1. AQE does **not** convert BNLJ to a cheaper strategy — it only optimises equi-joins.
2. For range joins on large datasets, consider the `RANGE_JOIN` hint (Databricks) or bucketing both sides by range boundaries.
3. Use `EXPLAIN` to confirm BNLJ is chosen; look for `BroadcastNestedLoopJoin` in the plan.
