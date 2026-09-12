# :material-grid: Shuffle-Replicate Nested-Loop / Cartesian Path

`SHUFFLE_REPLICATE_NL` is a Spark hint and planner strategy family, but Spark 4.2 typically exposes the resulting physical node as `CartesianProduct` rather than a separate operator named `ShuffleReplicateNestedLoopJoin`.

### :material-animation-play: Interactive Visualization — Cartesian and Replicated Nested-Loop Path

<div id="viz-joins-strategy-srnl" class="ts-viz"></div>

Use this comparison to distinguish the planner hint name from the actual physical operator name that appears in `EXPLAIN FORMATTED`.

<script src="../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-check-decagram: Verified in PySpark 4.2

With broadcast disabled:

```sql
select *
from range(0, 1000) a
cross join range(0, 200) b;
```

Spark showed:

```text
CartesianProduct
Join type: Inner
```

And with an equi-join plus `SHUFFLE_REPLICATE_NL`:

```sql
select /*+ shuffle_replicate_nl(s) */ *
from (select id, id % 5 as k from range(0, 1000)) b
join (select id, id % 5 as k from range(0, 200)) s
  on b.k = s.k;
```

Spark still showed `CartesianProduct`, with the equality predicate attached as the join condition.

______________________________________________________________________

## :material-information-outline: What This Page Really Means

This page exists because older articles often use names like SRNLJ or SRNLP. In Spark 4.2 terminology:

| Term                          | What it refers to                                           |
| ----------------------------- | ----------------------------------------------------------- |
| `SHUFFLE_REPLICATE_NL`        | A join hint                                                 |
| Shuffle-replicate nested-loop | A planner strategy idea                                     |
| `CartesianProduct`            | The physical operator name you will usually see in the plan |

______________________________________________________________________

## :material-code-tags: Cross-Join Controls

```sql
-- Explicit cartesian join: legal without extra config
select *
from a
cross join b;

-- Implicit cartesian join: needs a config change
set spark.sql.crossJoin.enabled = true;
select * from a join b;
```

When `spark.sql.crossJoin.enabled=false`, a conditionless `JOIN` raises an `AnalysisException` instead of planning a cartesian operator.

______________________________________________________________________

## :material-alert-outline: Performance Characteristics

- Output can grow to `N x M` rows very quickly.
- No hash-table probe or merge shortcut exists here.
- Broadcasting a small side can change the plan from `CartesianProduct` to `BroadcastNestedLoopJoin`.

______________________________________________________________________

## :material-lightbulb-outline: When to Use

Use this path only when cartesian semantics are truly required or when a non-equi shape leaves Spark no cheaper physical alternative.
