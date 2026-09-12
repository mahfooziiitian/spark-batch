# :material-broadcast: Broadcast Hash Join

`BroadcastHashJoin` is Spark 4.2's map-side equi-join operator for cases where one input is small enough to ship to every executor.

### :material-animation-play: Interactive Visualization — Broadcast Hash Join

<div id="viz-joins-strategy-bhj" class="ts-viz"></div>

This view highlights the build side, the streamed side, and the settings that determine when auto broadcast happens.

<script src="../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-check-decagram: Verified in PySpark 4.2

With:

```sql
select *
from (select id, id % 5 as k from range(0, 1000)) b
join (select id, id % 5 as k from range(0, 10)) s
  on b.k = s.k;
```

`EXPLAIN FORMATTED` showed:

```text
BroadcastExchange
BroadcastHashJoin [k#0L], [k#1L], Inner, BuildRight
```

When `spark.sql.autoBroadcastJoinThreshold = -1`, the same query switched to `SortMergeJoin`.

______________________________________________________________________

## :material-information-outline: What Triggers It

`BroadcastHashJoin` needs all of the following:

- An equi-join predicate.
- A supported join type.
- One side small enough to broadcast, or an explicit broadcast hint.

Practical triggers:

| Trigger                                                                  | Result                                                             |
| ------------------------------------------------------------------------ | ------------------------------------------------------------------ |
| Small estimated side under `spark.sql.autoBroadcastJoinThreshold`        | Spark may auto-pick `BroadcastHashJoin`                            |
| `/*+ BROADCAST(t) */`, `/*+ BROADCASTJOIN(t) */`, or `/*+ MAPJOIN(t) */` | Forces the named side into the broadcast family when legal         |
| `spark.sql.autoBroadcastJoinThreshold=-1`                                | Disables only automatic broadcasting, not explicit broadcast hints |

______________________________________________________________________

## :material-cog-outline: Important Constraints

- `BroadcastHashJoin` is for equi-joins, not `<`, `>`, `BETWEEN`, or arbitrary predicates.
- Spark 4.2 did not use `BroadcastHashJoin` for a tested `FULL OUTER JOIN`, even with a broadcast hint; it planned `SortMergeJoin` instead.
- The broadcast relation must fit in executor memory, and Spark materializes it on the driver first.

______________________________________________________________________

## :material-code-tags: Useful Hints and Settings

```sql
select /*+ broadcast(dim) */
    f.order_id,
    d.category
from fact_orders f
join dim_product d
  on f.product_id = d.product_id;

set spark.sql.autoBroadcastJoinThreshold = -1;
```

______________________________________________________________________

## :material-table: Why It Is Fast

| Property              | Broadcast hash join behavior                             |
| --------------------- | -------------------------------------------------------- |
| Shuffle on large side | Avoided                                                  |
| Shuffle on small side | Replaced by broadcast exchange                           |
| Build structure       | In-memory hash table                                     |
| Best fit              | Fact-to-small-dimension equi-joins                       |
| Common failure mode   | OOM from broadcasting something that is not really small |

______________________________________________________________________

## :material-alert-outline: Common Misreads

!!! warning "Broadcast does not mean zero network traffic"

    Spark still sends the small side across the cluster. The gain is that the large side does not need a matching shuffle.

!!! note "Size estimates matter"

    If Spark lacks good statistics, it may miss an otherwise good broadcast opportunity. Verify with `EXPLAIN FORMATTED` and add a hint only when needed.

______________________________________________________________________

## :material-lightbulb-outline: When to Use

Use `BroadcastHashJoin` when one side is genuinely small, the predicate is equality-based, and you want to avoid shuffling a much larger fact table.
