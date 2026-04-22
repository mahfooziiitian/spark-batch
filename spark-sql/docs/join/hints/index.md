# :material-lightbulb-on: Join Hints

Join hints let you influence the physical join strategy (e.g., broadcast).
They are useful for performance tuning when the optimizer makes suboptimal
choices.


### :material-sitemap: Overview

```mermaid
graph LR
    Q["SELECT /*+ BROADCAST(t) */ ..."] --> P[Planner]
    P --> J[Forced Broadcast Join]
```

---

## :material-pin: Common Hints

| Hint | Effect |
|------|--------|
| `BROADCAST` | Force broadcast join |
| `MERGE` | Prefer sort-merge join |
| `SHUFFLE_HASH` | Prefer shuffle hash join |
| `SHUFFLE_REPLICATE_NL` | Force Cartesian / nested loop |

---

## :material-flask-outline: Example

```sql
SELECT /*+ BROADCAST(dim) */
  f.order_id, dim.region
FROM fact_orders f
JOIN dim_region dim
ON f.region_id = dim.id;
```

---

## :material-magnify: Behavior Notes

1. Hints are best-effort; invalid hints are ignored.
2. Broadcast joins require the build side to fit in memory.
3. Use `EXPLAIN` to verify the chosen strategy.

---

### Related Guides

- [Hint Resolution](resolver.md)
- [Join Hint Operator](operator.md)
- [Range Join Hint](range_join_hint.md)
