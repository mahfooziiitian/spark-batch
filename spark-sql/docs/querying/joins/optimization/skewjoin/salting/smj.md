# :material-sort: Salting Before a Sort-Merge Join

There is no separate Spark 4.2 operator named "Salted Sort-Merge Join." After salting, Spark still plans a normal equi-join, often `SortMergeJoin`, over the transformed key set.

### :material-animation-play: Interactive Visualization — Salted Keys Feeding Sort-Merge Join

<div id="viz-joins-skew-salted-smj" class="ts-viz"></div>

This diagram shows the important distinction: salting changes the data shape, not the operator name.

<script src="../../../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-check-decagram: Verified in PySpark 4.2

With broadcast disabled, a salted join on `(customer_id, salt)` produced:

```text
SortMergeJoin [customer_id#0, salt#2], [customer_id#3, salt#5], Inner
```

So the execution strategy remained an ordinary `SortMergeJoin` over two equality keys.

______________________________________________________________________

## :material-information-outline: Why Combine Salting with SMJ

This pattern is useful when:

- The smaller side is not suitable for broadcast.
- You still need a shuffle-based equi-join.
- One key is hot enough that unsalted shuffle partitions become imbalanced.

Salting breaks one hot key into several smaller `(key, salt)` groups before the sort and merge stages happen.

______________________________________________________________________

## :material-code-tags: Example Shape

```sql
with fact as (
    select customer_id, txn, pmod(hash(txn), 4) as salt
    from fact_orders
),
dim as (
    select customer_id, segment, salt
    from dim_customer
    lateral view explode(sequence(0, 3)) s as salt
)
select *
from fact f
join dim d
  on f.customer_id = d.customer_id
 and f.salt = d.salt;
```

______________________________________________________________________

## :material-alert-outline: What This Does Not Mean

- Salting does not create a new Spark join operator.
- Salting does not remove shuffle; it redistributes it.
- Salting is unnecessary overhead when broadcast or ordinary AQE already solves the problem.

______________________________________________________________________

## :material-lightbulb-outline: When to Use

Use salted sort-merge as a manual fallback when you still want a normal shuffle-based equi-join, but the unsalted key distribution is too uneven for good parallelism.
