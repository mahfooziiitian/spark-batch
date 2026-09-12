# :material-cog-transfer: Join Strategy Overview

Spark 4.2 does not use one physical join operator for every query. Catalyst chooses among broadcast-hash, shuffle-hash, sort-merge, broadcast nested-loop, or cartesian execution based on predicate shape, relation size, hints, and a few key settings.

### :material-animation-play: Interactive Visualization — Strategy Selector

<div id="viz-joins-strategy-overview" class="ts-viz"></div>

Switch between verified join shapes to see which Spark 4.2 operator they produced and what made Catalyst choose them.

<script src="../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-check-decagram: Verified PySpark 4.2 Outcomes

| Query shape                               | Key settings used during verification                   | Verified physical operator                        |
| ----------------------------------------- | ------------------------------------------------------- | ------------------------------------------------- |
| Equi-join, small right side               | default `spark.sql.autoBroadcastJoinThreshold=10485760` | `BroadcastHashJoin`                               |
| Same equi-join, auto broadcast disabled   | `spark.sql.autoBroadcastJoinThreshold=-1`               | `SortMergeJoin`                                   |
| Non-equi join with broadcastable side     | default threshold                                       | `BroadcastNestedLoopJoin`                         |
| Equi-join with `SHUFFLE_HASH` hint        | broadcast disabled                                      | `ShuffledHashJoin`                                |
| Explicit `CROSS JOIN`, broadcast disabled | `spark.sql.autoBroadcastJoinThreshold=-1`               | `CartesianProduct`                                |
| Explicit `CROSS JOIN`, small right side   | default threshold                                       | `BroadcastNestedLoopJoin` with `Join type: Cross` |

______________________________________________________________________

## :material-source-branch: What Actually Drives Selection

### Equi-joins

For joins expressed with equality predicates such as `a.k = b.k`, Spark 4.2 can choose a hash-based or merge-based strategy:

1. `BROADCAST`/`MAPJOIN` hints can force `BroadcastHashJoin` when the join type supports it.
2. `SHUFFLE_HASH` can force `ShuffledHashJoin`.
3. `MERGE` can force `SortMergeJoin`.
4. Without hints, Spark may auto-broadcast a sufficiently small side.
5. If broadcasting is not used, Spark commonly falls back to `SortMergeJoin`.
6. `spark.sql.join.preferSortMergeJoin=false` makes shuffle-hash more eligible, but it does not guarantee `ShuffledHashJoin`; in local PySpark 4.2 checks, Spark still chose `SortMergeJoin` until a `SHUFFLE_HASH` hint was added.

### Non-equi joins and cartesian shapes

Predicates such as `<`, `>`, `BETWEEN`, `!=`, `LIKE`, or missing predicates cannot use the equi-join operators above:

1. If one side can be broadcast, Spark can use `BroadcastNestedLoopJoin`.
2. If explicit cartesian semantics are allowed and no side is broadcast, Spark 4.2 uses `CartesianProduct`.
3. The `SHUFFLE_REPLICATE_NL` hint belongs to this family, but the physical plan still shows `CartesianProduct`, not a separate `ShuffleReplicateNestedLoopJoin` node.

______________________________________________________________________

## :material-tune: Settings Worth Knowing

| Setting                                | Effect                                                                                                                   |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `spark.sql.autoBroadcastJoinThreshold` | Controls automatic broadcasting. `-1` disables auto broadcast.                                                           |
| `spark.sql.join.preferSortMergeJoin`   | Biases Catalyst toward `SortMergeJoin` over `ShuffledHashJoin`; `false` is only a preference, not a force.               |
| `spark.sql.crossJoin.enabled`          | Required for implicit cartesian joins such as `FROM a JOIN b` with no condition. Explicit `CROSS JOIN` does not need it. |
| `spark.sql.adaptive.enabled`           | Enables AQE, which can re-optimize shuffle stages after runtime statistics arrive.                                       |

______________________________________________________________________

## :material-code-tags: Minimal Verification Queries

```sql
-- Broadcast hash join
select *
from range(0, 1000) b(id)
join range(0, 10) s(id)
  on b.id % 5 = s.id % 5;

-- Broadcast nested-loop join
select *
from range(0, 1000) b(id)
join range(0, 10) s(id)
  on b.id % 5 < s.id % 5;

-- Shuffle hash join
select /*+ shuffle_hash(s) */ *
from range(0, 1000) b(id)
join range(0, 200) s(id)
  on b.id % 5 = s.id % 5;

-- Cartesian path
select *
from range(0, 1000) a
cross join range(0, 200) b;
```

______________________________________________________________________

## :material-lightbulb-outline: Reading the Plan Correctly

- Look for the operator name itself: `BroadcastHashJoin`, `BroadcastNestedLoopJoin`, `ShuffledHashJoin`, `SortMergeJoin`, or `CartesianProduct`.
- For broadcast strategies, check `BuildLeft` or `BuildRight` to see which side Spark materialized in memory.
- For shuffle-based joins, look for `Exchange` and `Sort` nodes above the join.
- For adaptive queries, inspect the executed plan after an action, not just the initial logical plan.
