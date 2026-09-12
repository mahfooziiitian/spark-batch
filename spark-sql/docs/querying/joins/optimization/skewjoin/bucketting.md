# :material-bucket-outline: Bucketing to Reduce Join Shuffle

Bucketing does not magically remove skew, but it can keep both sides of a repeated equi-join co-located so Spark avoids a new shuffle stage.

### :material-animation-play: Interactive Visualization — Bucketed Join Path

<div id="viz-joins-skew-bucketing" class="ts-viz"></div>

The view below separates what bucketing can eliminate from what Spark may still need to do before the join starts.

<script src="../../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-check-decagram: Verified in PySpark 4.2

Two tables written with `bucketBy(4, 'user_id')` and joined on `user_id` produced a physical plan with:

```text
Scan parquet ... Bucketed: true
Scan parquet ... Bucketed: true
SortMergeJoin
```

The plan contained no `Exchange` nodes, which confirms shuffle avoidance. It **did** contain `Sort` nodes, so bucketing alone did not remove sorting in this check.

______________________________________________________________________

## :material-information-outline: What Bucketing Actually Buys You

| Benefit                          | What it means                                                                     |
| -------------------------------- | --------------------------------------------------------------------------------- |
| Matching bucket layout           | Rows with the same key are already grouped into corresponding files or partitions |
| Fewer shuffles on repeated joins | Spark can often skip repartitioning work                                          |
| More predictable IO shape        | Helpful for large, recurring joins on the same keys                               |

What it does **not** guarantee:

- Perfect balance for a genuinely hot key.
- Removal of sort work.
- Better performance for one-off ad hoc joins.

______________________________________________________________________

## :material-code-tags: Example Write Pattern

```sql
create table users_bucketed
using parquet
clustered by (user_id) into 8 buckets
as select * from users;

create table orders_bucketed
using parquet
clustered by (user_id) into 8 buckets
as select * from orders;
```

______________________________________________________________________

## :material-alert-outline: Skew Caveat

If one key dominates the data, all rows for that key still hash to the same bucket number. Bucketing helps shuffle avoidance more directly than it helps severe single-key skew.

______________________________________________________________________

## :material-lightbulb-outline: When to Use

Use bucketing for stable, repeated equi-joins on large tables where avoiding shuffle is valuable and the data layout can be planned ahead of time.
