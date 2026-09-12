# :material-sort: Sort-Merge Join

`SortMergeJoin` is the most common shuffle-based equi-join plan in Spark 4.2 when broadcast is not used.

### :material-animation-play: Interactive Visualization — Sort-Merge Join

<div id="viz-joins-strategy-smj" class="ts-viz"></div>

See how both inputs shuffle and sort on the join key before the merge step walks the aligned partitions.

<script src="../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-check-decagram: Verified in PySpark 4.2

After disabling auto broadcast:

```sql
set spark.sql.autoBroadcastJoinThreshold = -1;

select *
from (select id, id % 5 as k from range(0, 1000)) b
join (select id, id % 5 as k from range(0, 10)) s
  on b.k = s.k;
```

`EXPLAIN FORMATTED` showed:

```text
SortMergeJoin [k#4L], [k#5L], Inner
Sort [k#4L ASC NULLS FIRST]
Sort [k#5L ASC NULLS FIRST]
```

The `MERGE` hint produced the same operator in a second verification query.

______________________________________________________________________

## :material-information-outline: When Spark Chooses It

Spark 4.2 tends to use `SortMergeJoin` when:

- The predicate is an equi-join.
- Broadcast is unavailable, disabled, or not selected.
- Keys are orderable.
- No stronger hint pushes Spark toward another eligible operator.

It is the safe general-purpose distributed join because it can stream sorted partitions without building a large hash table for the whole relation.

______________________________________________________________________

## :material-table: Properties

| Property        | Sort-merge join behavior                                           |
| --------------- | ------------------------------------------------------------------ |
| Predicate shape | Equi-join only                                                     |
| Shuffle         | Both sides                                                         |
| Sort            | Required on both sides                                             |
| Memory profile  | Lower than hash joins because the merge phase streams sorted input |
| Common use      | Large-to-large joins                                               |

______________________________________________________________________

## :material-code-tags: Useful Controls

```sql
set spark.sql.autoBroadcastJoinThreshold = -1;
set spark.sql.join.preferSortMergeJoin = true;

select /*+ merge(b, s) */ *
from big_fact b
join large_dim s
  on b.k = s.k;
```

______________________________________________________________________

## :material-alert-outline: Nuance That Matters

- `SortMergeJoin` is still an equi-join operator; it is not the plan for arbitrary range predicates.
- Bucketing can remove shuffle requirements, but Spark may still insert `Sort` nodes unless it can also trust the required ordering.
- If you care about the final adaptive choice, inspect the executed plan after the query runs.

______________________________________________________________________

## :material-lightbulb-outline: When to Use

Expect `SortMergeJoin` to be the baseline comparison point for large equi-joins, especially when neither side is small enough to broadcast safely.
