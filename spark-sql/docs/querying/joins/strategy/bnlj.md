# :material-vector-polyline: Broadcast Nested-Loop Join

`BroadcastNestedLoopJoin` is Spark 4.2's broadcast-based fallback for join shapes that cannot use the equi-join operators, especially non-equi predicates and some cartesian cases.

### :material-animation-play: Interactive Visualization — Broadcast Nested-Loop Join

<div id="viz-joins-strategy-bnlj" class="ts-viz"></div>

Toggle between non-equi and cross-style cases to see when Spark keeps the nested-loop path local by broadcasting one side.

<script src="../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-check-decagram: Verified in PySpark 4.2

For:

```sql
select *
from (select id, id % 5 as k from range(0, 1000)) b
join (select id, id % 5 as k from range(0, 10)) s
  on b.k < s.k;
```

`EXPLAIN FORMATTED` showed:

```text
BroadcastExchange
BroadcastNestedLoopJoin Inner BuildRight
Join condition: (k#8L < k#9L)
```

A tested `LEFT OUTER` non-equi join also produced `BroadcastNestedLoopJoin`, and a tested `FULL OUTER` non-equi join produced `BroadcastNestedLoopJoin FullOuter`.

______________________________________________________________________

## :material-information-outline: When Spark Uses It

Typical triggers:

| Pattern                                                  | Why nested-loop is needed                                                  |
| -------------------------------------------------------- | -------------------------------------------------------------------------- |
| `a.x < b.y`, `BETWEEN`, or other non-equality predicates | Hash and merge joins require equi-join keys                                |
| Explicit `CROSS JOIN` with a broadcastable side          | Spark can still broadcast one input and evaluate every combination locally |
| `BROADCAST` hint on a non-equi join                      | Forces the broadcast nested-loop family                                    |

If neither side can be broadcast, Spark may fall back to `CartesianProduct` instead.

______________________________________________________________________

## :material-code-tags: Verified Query Shapes

```sql
select /*+ broadcast(s) */ *
from big_table b
join small_ranges s
  on b.metric between s.min_metric and s.max_metric;

select *
from products
cross join regions;
```

______________________________________________________________________

## :material-table: Properties

| Property              | Broadcast nested-loop behavior                          |
| --------------------- | ------------------------------------------------------- |
| Predicate support     | Works with non-equi and cross joins                     |
| Broadcast requirement | One side should be small enough or explicitly hinted    |
| Complexity            | Compares each streamed row with many broadcast rows     |
| Memory risk           | Broadcast side must fit in memory                       |
| Physical plan clue    | `BroadcastNestedLoopJoin` plus `BuildLeft`/`BuildRight` |

______________________________________________________________________

## :material-alert-outline: What Not to Assume

- It is not limited to inner joins; tested `LeftOuter` and `FullOuter` plans also worked.
- It is usually much more expensive than broadcast-hash because Spark cannot probe a hash table on a single equality key.
- Explicit `CROSS JOIN` does not depend on `spark.sql.crossJoin.enabled`; only implicit cartesian joins do.

______________________________________________________________________

## :material-lightbulb-outline: When to Use

Treat `BroadcastNestedLoopJoin` as a necessary fallback when your predicate is inherently non-equi and one side is still small enough to distribute safely.
