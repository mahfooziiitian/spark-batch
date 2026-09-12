# :material-speedometer: Query Optimization as a SQL Problem

Traditional database SQL is (mostly) declarative: you write *what* you want, the optimizer
decides *how*, and the cost model is usually invisible to the query author. **Spark SQL is
different** — the same logical query can compile to physical plans with radically different
costs depending on data layout, statistics, join hints, and configuration. Understanding
execution is not optional tuning; it is part of writing correct-and-fast Spark SQL.

This page treats optimization the same way the rest of this reference treats any other SQL
problem family: **business problem → bad SQL → correct SQL → diagnostic SQL → `EXPLAIN FORMATTED` → execution behavior → optimization**.

## :material-sitemap: The Execution Concepts

| Concept                                 | Layer              | Deep-dive page                                                                                     |
| --------------------------------------- | ------------------ | -------------------------------------------------------------------------------------------------- |
| Partition pruning                       | Storage            | [Predicate & Partition Pruning](../../querying/filter/pp.md)                                       |
| Predicate pushdown                      | Catalyst / storage | [Predicate Pushdown](../../optimization/predicate-pushdown.md)                                     |
| Column pruning                          | Catalyst           | [Predicate Pushdown](../../optimization/predicate-pushdown.md)                                     |
| Dynamic partition pruning (DPP)         | Runtime / AQE      | [Predicate & Partition Pruning](../../querying/filter/pp.md)                                       |
| Broadcast join (`BroadcastHashJoin`)    | Physical planning  | [BHJ Strategy](../../querying/joins/strategy/bhj.md)                                               |
| Sort-merge join (`SortMergeJoin`)       | Physical planning  | [SMJ Strategy](../../querying/joins/strategy/smj.md)                                               |
| Shuffled hash join (`ShuffledHashJoin`) | Physical planning  | [SHJ Strategy](../../querying/joins/strategy/shj.md)                                               |
| Shuffle / `Exchange`                    | Execution          | [Shuffling](../../optimization/shuffling.md)                                                       |
| Adaptive Query Execution (AQE)          | Runtime            | [AQE](../../optimization/aqe/index.md)                                                             |
| Skew handling                           | Runtime (AQE)      | [Optimizing Skew Join](../../optimization/aqe/optimizing-skew-join.md)                             |
| SMJ → Broadcast at runtime              | Runtime (AQE)      | [Converting SMJ to Broadcast](../../optimization/aqe/converting-sort-merge-join-broadcast-join.md) |
| Repartition vs. coalesce                | Execution          | See [below](#pattern-1-repartition-vs-coalesce-shuffle-or-not)                                     |
| File pruning                            | Storage            | [Partitioning](../../optimization/partition/index.md)                                              |
| `EXPLAIN FORMATTED` / profiling         | Diagnostics        | [Profiling](../../optimization/profiling.md), [Explain](../../optimization/execution/explain.md)   |

The rest of this page works through patterns that don't already have a dedicated
before/after treatment elsewhere: repartition-vs-coalesce, recomputed subqueries, and
UDF-blocked pushdown.

## :material-close-circle: Pattern 1 — `REPARTITION` vs `COALESCE` (shuffle or not)

**Business problem**: an ETL job writes a daily partition and someone adds
`.repartition(4)` "to reduce the number of output files," which now costs a full shuffle
on every run.

=== "Bad: unnecessary shuffle"

    ```sql
    -- Only reducing file count — REPARTITION shuffles all rows
    SELECT /*+ REPARTITION(4) */ * FROM daily_events;
    ```

=== "Correct: reduce without shuffling"

    ```sql
    -- COALESCE merges existing partitions in place — no shuffle
    SELECT /*+ COALESCE(4) */ * FROM daily_events;
    ```

!!! success "Verified: `COALESCE` has no `Exchange`, `REPARTITION` does"

    ```text
    -- df.repartition(4)
    == Physical Plan ==
    +- Exchange RoundRobinPartitioning(4), REPARTITION_BY_NUM
       +- Range (0, 1000, step=1, splits=8)

    -- df.coalesce(2)
    == Physical Plan ==
    Coalesce 2
    +- Range (0, 1000, step=1, splits=8)
    ```

    `COALESCE` is the right call whenever you only need **fewer, not more, partitions**
    and don't need to rebalance skewed data — it merges adjacent tasks without a shuffle.
    Use `REPARTITION` (or `REBALANCE` under AQE) only when you actually need to
    redistribute rows across a new partitioning key, or need *more* partitions than you
    currently have (coalesce cannot increase partition count).

## :material-close-circle: Pattern 2 — Recomputed Identical Subqueries

**Business problem**: a report computes the same aggregate twice (once for a raw value,
once for a derived ratio) and both copies get planned — and executed — independently.

=== "Bad: same subquery written twice"

    ```sql
    SELECT
        (SELECT SUM(id) FROM t) AS total1,
        (SELECT SUM(id) FROM t) / 2 AS half
    FROM (SELECT 1);
    ```

=== "Correct: compute once, reuse via CTE"

    ```sql
    WITH totals AS (
        SELECT SUM(id) AS total FROM t
    )
    SELECT total AS total1, total / 2 AS half
    FROM totals;
    ```

!!! success "Verified: two identical `Subquery` sub-plans, each with its own `Exchange`"

    `EXPLAIN` on the "bad" version shows **two separate** `subquery#9` / `subquery#11`
    branches, each independently re-running `Range → HashAggregate → Exchange SinglePartition → HashAggregate`:

    ```text
    :- Subquery subquery#9 ...
    :  +- HashAggregate(functions=[sum(id)])
    :     +- Exchange SinglePartition
    :        +- HashAggregate(functions=[partial_sum(id)])
    :           +- Range (0, 1000, step=1, splits=8)
    +- Subquery subquery#11 ...
       +- HashAggregate(functions=[sum(id)])
          +- Exchange SinglePartition
             +- HashAggregate(functions=[partial_sum(id)])
                +- Range (0, 1000, step=1, splits=8)
    ```

    Catalyst does **not** deduplicate identical scalar subqueries written independently in
    SQL text — a `WITH` CTE materializes the computation exactly once and both references
    reuse the same plan node.

## :material-close-circle: Pattern 3 — Function on a Filtered Column Blocks Pushdown

**Business problem**: a dashboard filters on a derived expression, silently forcing a full
table scan even though the underlying column is highly selective.

=== "Bad: UDF wraps the column"

    ```sql
    -- Python/Scala UDFs are opaque to Catalyst: cannot push down, cannot prune
    SELECT * FROM orders WHERE is_target(region);
    ```

=== "Correct: inline predicate Catalyst understands"

    ```sql
    SELECT * FROM orders WHERE region = 2;
    ```

!!! note "Built-in functions are not always the problem — user code is"

    `CAST`, `YEAR`, `DATE_FORMAT` and other **built-in** functions are known to Catalyst
    and can still participate in partition pruning and pushdown in many cases (see
    [Predicate & Partition Pruning](../../querying/filter/pp.md) for the verified
    exceptions). It is **opaque user code** — Python UDFs, non-deterministic functions
    (`RAND()`), and closures — that Catalyst genuinely cannot see through. Prefer built-in
    SQL expressions over UDFs on any column used in `WHERE`/`JOIN`.

## :material-flash: Diagnostic Workflow

When a query is "slow," work through these `EXPLAIN FORMATTED` checks in order — this
mirrors the [debugging workflow](../../querying/joins/issues/debugging-workflow.md) used
for join-specific problems, generalized to any query:

1. **`PartitionFilters`** in the scan node — are partition columns actually being used to
    skip files? Absent partition filters on a partitioned table is the first red flag.
2. **`PushedFilters`** in the scan node — did row-group/stripe-level filters make it to the
    storage format (Parquet/ORC)? A UDF or unsupported expression breaks this.
3. **Join node type** — `BroadcastHashJoin`, `SortMergeJoin`, or `ShuffledHashJoin`? Confirm
    the smaller side is the one being broadcast, and that the broadcast size is under
    `spark.sql.autoBroadcastJoinThreshold`.
4. **`Exchange` nodes** — every `Exchange` is a shuffle boundary. Count them; each one is a
    full network shuffle. Ask whether a `REPARTITION`/`COALESCE` hint, bucketing, or a join
    reorder can remove one.
5. **AQE re-optimization** — in the Spark UI SQL tab, compare the *submitted* plan to the
    *final* (adaptive) plan. AQE may have converted a `SortMergeJoin` to
    `BroadcastHashJoin`, split skewed partitions, or coalesced shuffle output — see
    [AQE](../../optimization/aqe/index.md).
6. **Spill / OOM** — check the Spark UI's "Spill (Memory)" / "Spill (Disk)" task metrics;
    large spills indicate a partition is too large for its allotted executor memory, often
    caused by skew (see [Skew handling](../../optimization/aqe/optimizing-skew-join.md)).

## :material-brain: When to Use

| Symptom                                               | Likely cause                                                           | Fix                                                                            |
| ----------------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| Full scan despite a filter on a partition column      | Function/UDF wraps the column                                          | Rewrite as a plain comparison; avoid UDFs in `WHERE`                           |
| Join always chooses `SortMergeJoin` for a small table | Table exceeds broadcast threshold or stats are stale                   | `/*+ BROADCAST(t) */` hint or `ANALYZE TABLE`                                  |
| One task takes far longer than the rest               | Data skew on the join/group key                                        | Salting, `/*+ SKEW('t', 'col', (...)) */` hint, or AQE skew join               |
| Many tiny output files                                | Over-partitioned write, no coalescing                                  | `/*+ COALESCE(n) */` or `REBALANCE`                                            |
| Same aggregate computed twice in one query            | Duplicate subquery text                                                | Factor into a `WITH` CTE                                                       |
| Query plan has more `Exchange` nodes than expected    | Unnecessary `REPARTITION`, join order, or aggregation before filtering | Push filters earlier; reorder joins; use `COALESCE` where shuffle isn't needed |

## :material-link-variant: Related

- [Optimization Overview](../../optimization/index.md) — layers, techniques-at-a-glance, anti-patterns table.
- [Join Strategies](../../querying/joins/strategy/index.md) — BHJ/SMJ/SHJ selection rules.
- [Join Issues & Troubleshooting Matrix](../../querying/joins/issues/troubleshooting-matrix.md) — symptom-to-fix table for join-specific problems.
- [AQE](../../optimization/aqe/index.md) — runtime re-optimization: skew splitting, join conversion, partition coalescing.
