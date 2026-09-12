# :material-swap-horizontal-bold: Shuffled Hash Join

`ShuffledHashJoin` is Spark 4.2's partition-local hash join for equi-joins after both sides have been repartitioned by the join key.

### :material-animation-play: Interactive Visualization — Shuffled Hash Join

<div id="viz-joins-strategy-shj" class="ts-viz"></div>

This visualization contrasts shuffle-hash with sort-merge so you can see the trade-off between building a hash table and sorting both sides.

<script src="../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-check-decagram: Verified in PySpark 4.2

For:

```sql
select /*+ shuffle_hash(s) */ *
from (select id, id % 5 as k from range(0, 1000)) b
join (select id, id % 5 as k from range(0, 200)) s
  on b.k = s.k;
```

`EXPLAIN FORMATTED` showed:

```text
ShuffledHashJoin [k#12L], [k#13L], Inner, BuildRight
```

A tested `FULL OUTER JOIN` with a `SHUFFLE_HASH` hint also planned as `ShuffledHashJoin FullOuter`, which means Spark 4.2 supports more join types here than many older summaries claim.

______________________________________________________________________

## :material-information-outline: What Triggers It

`ShuffledHashJoin` needs an equi-join. Beyond that:

- `SHUFFLE_HASH` is the most direct way to request it.
- Without hints, Spark may still prefer `SortMergeJoin`.
- Setting `spark.sql.join.preferSortMergeJoin=false` only lowers the planner's bias toward sort-merge; it does not guarantee shuffle-hash.

In local PySpark 4.2 checks, Spark kept choosing `SortMergeJoin` for several unhinted equi-joins even after setting `spark.sql.join.preferSortMergeJoin=false` and disabling broadcast.

______________________________________________________________________

## :material-table: Properties

| Property         | Shuffled hash join behavior                       |
| ---------------- | ------------------------------------------------- |
| Predicate shape  | Equi-join only                                    |
| Shuffle          | Both sides shuffle by key                         |
| Sort requirement | None                                              |
| Memory profile   | Build side must fit as a per-partition hash table |
| Good fit         | Equi-joins where hashing is preferable to sorting |

______________________________________________________________________

## :material-code-tags: Practical Pattern

```sql
set spark.sql.autoBroadcastJoinThreshold = -1;

select /*+ shuffle_hash(dim) */
    f.order_id,
    d.category
from fact_orders f
join dim_product d
  on f.product_id = d.product_id;
```

______________________________________________________________________

## :material-alert-outline: Common Overstatements to Avoid

- "`preferSortMergeJoin=false` forces shuffle-hash" is false.
- "Shuffle-hash cannot do full outer joins" is false in Spark 4.2.
- "Shuffle-hash is always faster than sort-merge" is false; it trades sort cost for hash-table memory pressure.

______________________________________________________________________

## :material-lightbulb-outline: When to Use

Reach for `ShuffledHashJoin` when you have an equi-join, broadcasting is not desirable, and you want to test a hash-based shuffle plan explicitly with `SHUFFLE_HASH`.
