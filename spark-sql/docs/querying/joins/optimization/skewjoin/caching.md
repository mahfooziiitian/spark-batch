# :material-database-lock: Caching Around Skewed Joins

Caching is useful around skewed joins only when it prevents repeated scans, repeated filters, or repeated salting work. It is not itself a skew-distribution technique.

### :material-animation-play: Interactive Visualization — Cache Where Reuse Exists

<div id="viz-joins-skew-caching" class="ts-viz"></div>

Use this decision view to separate "cache for reuse" from "fix the skew" so you do not expect caching to solve the wrong problem.

<script src="../../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-information-outline: Good Use Cases

Cache after a costly but reusable step such as:

- Filtering a large fact table down to a much smaller working set.
- Computing a salted version of the skewed side that will feed multiple joins.
- Materializing the list of hot keys used for split-path processing.

______________________________________________________________________

## :material-code-tags: Example Pattern

```sql
cache table recent_orders;

select /*+ broadcast(dim) */
    o.order_id,
    dim.segment
from recent_orders o
join dim_customer dim
  on o.customer_id = dim.customer_id;
```

______________________________________________________________________

## :material-alert-outline: What Caching Does Not Do

- It does not redistribute a hot key.
- It does not turn a shuffle join into a broadcast join.
- It can make memory pressure worse if you cache the wrong large intermediate.

______________________________________________________________________

## :material-lightbulb-outline: When to Use

Cache only when the same expensive intermediate is reused by multiple downstream joins or analyses. For a one-time skewed join, fix the join shape first.
