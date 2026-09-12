# :material-broadcast: Broadcast-Assisted Skew Join (Informal "BMPJ")

"BMPJ" is not a Spark 4.2 physical operator name. It is a useful informal label for a simple idea: use a broadcast join so the skewed large side stays local and avoids a shuffle bottleneck.

### :material-animation-play: Interactive Visualization — Broadcast vs Shuffle Under Skew

<div id="viz-joins-skew-broadcast" class="ts-viz"></div>

This comparison shows why broadcasting a small dimension is often the fastest skew fix: it removes the shuffle stage that would have concentrated hot keys.

<script src="../../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-check-decagram: Verified Reality in PySpark 4.2

A normal equi-join with a broadcastable right side produced `BroadcastHashJoin`. Spark did **not** show an operator named `BMPJ`.

So this page documents a tuning pattern, not a distinct executor algorithm.

______________________________________________________________________

## :material-information-outline: When It Helps

Broadcast-assisted skew handling works best when:

- The small side really is small.
- The expensive side is skewed on the join key.
- The predicate is an equi-join.

Because the big side is not shuffled, a hot key does not create a single oversized reduce-style partition.

______________________________________________________________________

## :material-code-tags: Example Pattern

```sql
select /*+ broadcast(dim) */
    f.order_id,
    dim.segment
from fact_orders f
join dim_customer dim
  on f.customer_id = dim.customer_id;
```

______________________________________________________________________

## :material-alert-outline: Limits

- If the "small" side is not small, you just trade skew for memory pressure.
- This pattern does not help non-equi joins that fall into `BroadcastNestedLoopJoin`.
- It is still the ordinary broadcast join family, so validate with `BroadcastHashJoin` in the plan.

______________________________________________________________________

## :material-lightbulb-outline: When to Use

Use this as the first skew remedy whenever a dimension-style side can be broadcast safely. It is usually cheaper than salting, splitting keys, or chunking the join manually.
