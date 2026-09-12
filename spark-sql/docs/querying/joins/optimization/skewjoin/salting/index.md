# :material-shaker-outline: Salting a Skewed Join Key

Salting spreads one hot logical key across multiple physical join keys by adding a second component such as `salt`, then matching both the original key and the salt in the join condition.

### :material-animation-play: Interactive Visualization — Salt One Side, Expand the Other

<div id="viz-joins-skew-salting" class="ts-viz"></div>

This view shows the verified Spark pattern: add a salt to the skewed side and generate every salt value on the other side before joining on both columns.

<script src="../../../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-check-decagram: Verified in PySpark 4.2

A small verification query used:

- `pmod(hash(txn), 3)` to assign deterministic salt values on the skewed side.
- `explode(sequence(0, 2))` to duplicate the other side across all salt values.

The salted join returned exactly the same rows as the baseline unsalted join.

______________________________________________________________________

## :material-information-outline: Correct Salting Pattern

### 1. Add a salt to the skewed side

```sql
with salted_fact as (
    select
        customer_id,
        txn,
        pmod(hash(txn), 8) as salt
    from fact_orders
)
```

### 2. Expand the other side across the same salt range

```sql
, expanded_dim as (
    select
        customer_id,
        segment,
        salt
    from dim_customer
    lateral view explode(sequence(0, 7)) s as salt
)
```

### 3. Join on both key parts

```sql
select sf.txn, ed.segment
from salted_fact sf
join expanded_dim ed
  on sf.customer_id = ed.customer_id
 and sf.salt = ed.salt;
```

______________________________________________________________________

## :material-table: Random vs Deterministic Salt

| Approach                       | When it helps               | Trade-off                               |
| ------------------------------ | --------------------------- | --------------------------------------- |
| `floor(rand() * n)`            | Quick experiments           | Non-deterministic                       |
| `pmod(hash(stable_column), n)` | Reproducible jobs and tests | Requires a stable per-row source column |
| Selective salting              | Only a few known hot keys   | More branching logic                    |

______________________________________________________________________

## :material-alert-outline: Rules That Keep It Correct

- Only the skewed side gets one salt value per row.
- The other side must be expanded to **all** salt values.
- Join on `(original_key, salt)`, not just the original key.
- Re-aggregate on the original key afterward if your downstream logic expects the unsalted shape.

______________________________________________________________________

## :material-lightbulb-outline: When to Use

Use salting when one or a few keys are so dominant that AQE and ordinary repartitioning still leave a straggler task.
