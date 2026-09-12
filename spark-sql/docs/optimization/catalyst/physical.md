# :material-server: Physical Planning

Physical planning converts an optimized logical plan into executable `SparkPlan` operators. Spark can consider multiple strategies internally, but `EXPLAIN FORMATTED` shows the **selected** plan that will feed execution and, with AQE enabled, potentially later runtime re-optimization.

To avoid repeating the broader planner walkthrough in [Query Planner](../../internals/planner/query-planner.md), this page concentrates on concrete Spark 4.2 plan signatures you can verify today.

______________________________________________________________________

### :material-animation-play: Interactive Visualization — Strategy Selection Signals

<div id="viz-physical-strategy-tree" class="ts-viz"></div>

The planner's output is easiest to read by looking for a few signature nodes: `BroadcastExchange` for broadcast joins, paired `Exchange` + `Sort` nodes for a classic sort-merge join, and `partial_*` aggregate functions before the shuffle in two-phase aggregation.

______________________________________________________________________

## :material-call-split: Verified Example 1 — Broadcast Hash Join

Spark 4.2 query with a broadcast hint:

```sql
EXPLAIN FORMATTED
SELECT /*+ BROADCAST(small_t) */ count(*)
FROM big_t
JOIN small_t USING (k);
```

Observed operator chain:

```text
(5) BroadcastExchange
Input [1]: [k#15L]
Arguments: HashedRelationBroadcastMode(...)

(6) BroadcastHashJoin
Left keys [1]: [k#12L]
Right keys [1]: [k#15L]
Join type: Inner
```

That pairing is the key signature: Spark materialized the small side with `BroadcastExchange` and consumed it with `BroadcastHashJoin`.

______________________________________________________________________

## :material-source-branch: Verified Example 2 — Sort-Merge Join

Spark 4.2 query with broadcasting disabled and both inputs split into four partitions:

```sql
SET spark.sql.autoBroadcastJoinThreshold = -1;
SET spark.sql.shuffle.partitions = 4;

EXPLAIN FORMATTED
SELECT count(*)
FROM big_t
JOIN big_t2 USING (k);
```

Observed operator chain:

```text
(3) Exchange
Arguments: hashpartitioning(k#1L, 4), ENSURE_REQUIREMENTS, ...

(4) Sort
Arguments: [k#1L ASC NULLS FIRST], false, 0

(7) Exchange
Arguments: hashpartitioning(k#4L, 4), ENSURE_REQUIREMENTS, ...

(8) Sort
Arguments: [k#4L ASC NULLS FIRST], false, 0

(9) SortMergeJoin
Left keys [1]: [k#1L]
Right keys [1]: [k#4L]
Join type: Inner
```

This is the classic sort-merge shape: repartition both sides by key, sort both sides, then merge.

!!! note "Do not overfit to one skeleton"

    In a trivial single-partition local demo, Spark 4.2 still chose `SortMergeJoin` for the same query shape but did **not** need separate `Exchange` or `Sort` nodes because the input partitioning and ordering requirements were already satisfied. The join operator name is the authoritative clue.

______________________________________________________________________

## :material-sigma: Verified Example 3 — Two-Phase `HashAggregate`

Spark 4.2 query:

```sql
EXPLAIN FORMATTED
SELECT k % 3 AS g, count(*) AS c
FROM big_t
GROUP BY k % 3;
```

Observed operator chain:

```text
(3) HashAggregate
Keys [1]: [_groupingexpression#5L]
Functions [1]: [partial_count(1)]

(4) Exchange
Arguments: hashpartitioning(_groupingexpression#5L, 4), ENSURE_REQUIREMENTS, ...

(5) HashAggregate
Keys [1]: [_groupingexpression#5L]
Functions [1]: [count(1)]
```

This verifies the standard Spark aggregation pattern:

1. a **partial** aggregate runs before the shuffle,
2. grouped partial results are shuffled by key,
3. a final aggregate combines those partial counts.

______________________________________________________________________

## :material-file-tree-outline: What `EXPLAIN FORMATTED` Tells You Fastest

| Node or field                    | How to read it                                         |
| -------------------------------- | ------------------------------------------------------ |
| `BroadcastExchange`              | A side was materialized for broadcast                  |
| `BroadcastHashJoin`              | Join executed by probing a broadcast hash table        |
| `SortMergeJoin`                  | Join executed after meeting sort-order requirements    |
| `Exchange hashpartitioning(...)` | Shuffle boundary introduced                            |
| `HashAggregate` with `partial_*` | Pre-shuffle local aggregation                          |
| `AdaptiveSparkPlan`              | AQE wrapper is enabled and may revise the initial plan |

AQE was enabled in the verified examples above, so the root operator printed as `AdaptiveSparkPlan ... isFinalPlan=false`. That means the current explain output is the **initial** chosen plan, not necessarily the final runtime-adjusted one.

______________________________________________________________________

## :material-tune: Practical Tuning Cues

| If you see this                                   | First question to ask                                                |
| ------------------------------------------------- | -------------------------------------------------------------------- |
| `BroadcastHashJoin` absent for a very small table | Is broadcasting disabled, or are statistics/hints missing?           |
| `SortMergeJoin` plus two large exchanges          | Can one side be broadcast, or can upstream partitioning be improved? |
| Extra `Exchange` above a final aggregate          | Is the query collapsing to a single output partition?                |
| `AdaptiveSparkPlan` everywhere                    | Did AQE rewrite the plan after execution started?                    |

______________________________________________________________________

## :material-link-variant: See Also

- [Catalyst Optimizer](index.md)
- [Code Generation](code-generation.md)
- [Query Planner](../../internals/planner/query-planner.md)
