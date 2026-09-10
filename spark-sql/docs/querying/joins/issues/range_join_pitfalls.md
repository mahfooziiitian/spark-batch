# :material-arrow-left-right: Range / Non-Equality Join Pitfalls

A join whose condition uses `<`, `<=`, `>`, `>=`, `BETWEEN`, or `<>` instead of (or in
addition to) `=` is a **non-equi join**. These are indispensable for time-window,
interval, and nearest-match logic (see the
[Range Join](../types/non_equi_join/range_join/point_in_interval.md) reference for how to
write them well), but they have a sharp performance cliff: a condition with **no equality
component** cannot use a hash join. Spark falls back to a **nested-loop** strategy that
compares every left row against every right row — `O(N x M)` — which quietly becomes a
`CartesianProduct` when neither side is small enough to broadcast.

---

### :material-sitemap: Overview

```mermaid
graph TD
    C["Join condition"] --> E{"Has an equality (=) component?"}
    E -->|No -- pure range| NB{"One side broadcastable?"}
    NB -->|Yes| BNLJ["BroadcastNestedLoopJoin -- O(N x M), no shuffle but every pair compared"]
    NB -->|No| CP["CartesianProduct -- O(N x M) full explosion"]
    E -->|Yes, plus a range residual| HJ["BroadcastHashJoin / SortMergeJoin -- partition by key, range checked only within matches"]
    BNLJ -->|fix| ADD["Add an equi-key component"]
    CP -->|fix| ADD
    ADD --> HJ
```

---

## :material-pin: Common Symptoms

- `EXPLAIN` shows `BroadcastNestedLoopJoin` or `CartesianProduct` where you expected a
  `SortMergeJoin`/`BroadcastHashJoin` — the tell-tale sign of a pure non-equi join.
- A range join is orders of magnitude slower than an equi-join on the same tables, and
  gets dramatically worse as either side grows (quadratic, not linear).
- A one-sided range predicate (`ts >= w.lo` with no upper bound) matches far more rows
  than intended — every later row qualifies for every earlier window.
- The job runs fine on small samples but hangs or OOMs on full data, because `N x M`
  grew faster than either table alone.

---

## :material-flask-outline: Practical Examples

### Setup

```sql
CREATE TABLE readings (sensor_id INT, ts INT, val DOUBLE);
INSERT INTO readings VALUES
    (1, 100, 10.0), (1, 200, 11.0), (1, 300, 12.0), (2, 150, 20.0);

CREATE TABLE windows (sensor_id INT, win_id INT, lo INT, hi INT);
INSERT INTO windows VALUES
    (1, 1, 100, 250), (1, 2, 200, 400), (2, 3, 100, 200);
```

### Example 1 — The Cost: A Pure Range Join Is a Nested Loop

```sql
EXPLAIN SELECT r.ts, w.win_id
FROM readings r
JOIN windows w ON r.ts BETWEEN w.lo AND w.hi;
```

```text
+- BroadcastNestedLoopJoin BuildRight, Inner, ((ts#.. >= lo#..) AND (ts#.. <= hi#..))
```

There's no `=` in the condition, so Spark can't hash-partition the rows — it broadcasts
one side and compares **every** reading against **every** window. With N readings and M
windows that's `N x M` comparisons regardless of how selective the range actually is.

### Example 2 — Worse: No Broadcastable Side Becomes a Cartesian Product

```sql
SET spark.sql.autoBroadcastJoinThreshold = -1;   -- simulate two large tables

EXPLAIN SELECT r.ts, w.win_id
FROM readings r
JOIN windows w ON r.ts BETWEEN w.lo AND w.hi;
```

```text
+- CartesianProduct ((ts#.. >= lo#..) AND (ts#.. <= hi#..))
```

When neither side fits in a broadcast, the nested loop degrades to a full
`CartesianProduct` — the entire cross product is materialized and *then* filtered by the
range. This is the same physical operator as an [Accidental Cross Join](cartesian_join.md),
except here it's the *range condition itself*, not a missing `ON`, that forces it.

### Example 3 — The Fix: Add an Equality Component

```sql
EXPLAIN SELECT r.ts, w.win_id
FROM readings r
JOIN windows w ON r.sensor_id = w.sensor_id AND r.ts BETWEEN w.lo AND w.hi;
```

```text
+- BroadcastHashJoin [sensor_id#..], [sensor_id#..], Inner,
     ((ts#.. >= lo#..) AND (ts#.. <= hi#..)), ...
```

Adding `r.sensor_id = w.sensor_id` gives Spark an equi-key to hash/partition on; the
range becomes a **residual condition** evaluated only *within* rows that already share a
`sensor_id`. Instead of `N x M`, each reading is only compared against the windows for
its own sensor — the join is now near-linear in practice. The result is also correctly
scoped per sensor:

| sensor_id | ts | win_id |
|:---:|:---:|:---:|
| 1 | 100 | 1 |
| 1 | 200 | 1 |
| 1 | 200 | 2 |
| 1 | 300 | 2 |
| 2 | 150 | 3 |

### Example 4 — Correctness: Don't Leave a Range One-Sided

```sql
-- BUG: only a lower bound -- every reading matches every window that started before it,
-- with no upper cutoff.
SELECT r.sensor_id, r.ts, w.win_id
FROM readings r
JOIN windows w ON r.sensor_id = w.sensor_id AND r.ts >= w.lo;
-- Returns extra rows: e.g. sensor 1's ts=300 matches BOTH win 1 (100..) and win 2 (200..)
-- because neither upper bound (250 / 400) is being checked.

-- FIX: bound both ends so each reading falls inside a real interval.
SELECT r.sensor_id, r.ts, w.win_id
FROM readings r
JOIN windows w ON r.sensor_id = w.sensor_id AND r.ts BETWEEN w.lo AND w.hi;
```

A one-sided range isn't just slower — it's usually *wrong*: it silently over-matches.
Always confirm whether the intent is a bounded interval (`BETWEEN lo AND hi`) or a genuine
open-ended "as of" lookup (which typically needs a `QUALIFY ROW_NUMBER()` to pick the one
most-recent match — see [Incomplete Join Conditions](incomplete_join_conditions.md)).

---

## :material-brain: When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Range/interval match where rows also share a natural key | Always include the equi-key in `ON` alongside the range — turns a nested loop into a hash/sort-merge join (Example 3) |
| No natural equi-key, but ranges align to regular buckets | Derive a coarse bucket column (e.g. `ts / 3600`) on both sides and equi-join on it, keeping the exact range as a residual |
| Genuine open-ended "latest as of" lookup | Use a bounded range or `effective_date <= t` plus `QUALIFY ROW_NUMBER()` — never an unbounded one-sided range that over-matches (Example 4) |
| Pure range join is unavoidable and both sides are large | Expect `BroadcastNestedLoopJoin`/`CartesianProduct`; shrink one side (pre-filter/aggregate) so it broadcasts, or reduce M x N before joining |
| Small dimension of intervals vs. large fact stream | Let Spark broadcast the small side; verify with `EXPLAIN` it's `BroadcastNestedLoopJoin` (acceptable) and not a `CartesianProduct` (both sides large) |

!!! warning "Non-equi joins scale quadratically — validate on real data volumes"
    A range join that's instant on a 1,000-row sample can be catastrophic at
    10 million rows, because the work grows with `N x M`, not `N + M`. Before shipping a
    range join, run `EXPLAIN` to confirm the strategy, and add an equality component
    (Example 3) whenever the data model has one — it's the single most effective fix.
    See [Broadcast Join Pitfalls](broadcast_pitfalls.md) for the related risk of the
    broadcast side itself being too large.
