# :material-sigma: Aggregate HOF

`AGGREGATE(array, init, merge_lambda [, finish_lambda])` is Spark SQL's
**fold / reduce** higher-order function. It iterates over array elements,
accumulating a running state with `merge_lambda`, then optionally transforms
the final state with `finish_lambda`.

______________________________________________________________________

## :material-pin: Syntax

```sql
AGGREGATE(
    array,                         -- input array
    init_value,                    -- initial accumulator value
    (acc, element) -> new_acc,     -- merge lambda: called for each element
    [acc -> result]                -- finish lambda (optional): transform final acc
)
```

______________________________________________________________________

## :material-flask-outline: Examples

### :material-animation-play: Interactive Visualization

<div id="viz-aggregate-fold" class="ts-viz"></div>

Step through the fold to watch `acc` evolve one element at a time — this is
exactly what `AGGREGATE`'s merge lambda does internally before `finish_lambda`
runs once on the final value.

### Sum (equivalent to `AGGREGATE` = `array_sum`)

```sql
SELECT AGGREGATE(ARRAY(10, 20, 30, 40), 0, (acc, x) -> acc + x) AS total;
-- Result: 100
```

### Product

```sql
SELECT AGGREGATE(ARRAY(1, 2, 3, 4, 5), 1, (acc, x) -> acc * x) AS product;
-- Result: 120
```

### Average (merge + finish)

```sql
SELECT AGGREGATE(
    ARRAY(10, 20, 30, 40),
    NAMED_STRUCT('total', 0D, 'cnt', 0),
    (acc, x) -> NAMED_STRUCT('total', acc.total + x, 'cnt', acc.cnt + 1),
    acc -> acc.total / acc.cnt
) AS average;
-- Result: 25.0
```

### Max without built-in (educational)

```sql
SELECT AGGREGATE(
    ARRAY(7, 3, 9, 1, 5),
    CAST(NULL AS INT),
    (acc, x) -> CASE WHEN acc IS NULL OR x > acc THEN x ELSE acc END
) AS manual_max;
-- Result: 9
```

### String join (array to CSV)

```sql
SELECT AGGREGATE(
    ARRAY('alpha', 'beta', 'gamma'),
    '',
    (acc, s) -> CASE WHEN acc = '' THEN s ELSE acc || ',' || s END
) AS csv;
-- Result: 'alpha,beta,gamma'
```

### Weighted sum (parallel arrays + ZIP_WITH)

```sql
-- ZIP_WITH first to pair values with weights, then AGGREGATE
SELECT AGGREGATE(
    ZIP_WITH(ARRAY(100, 200, 150), ARRAY(0.5, 0.3, 0.2), (v, w) -> v * w),
    0D,
    (acc, x) -> acc + x
) AS weighted_sum;
-- Result: 50.0 + 60.0 + 30.0 = 140.0
```

### Running total accumulator — struct with history

```sql
-- Track both sum and count as a struct accumulator
SELECT AGGREGATE(
    line_item_prices,
    NAMED_STRUCT('subtotal', 0D, 'item_count', 0, 'avg_price', 0D),
    (acc, price) -> NAMED_STRUCT(
        'subtotal',   acc.subtotal + price,
        'item_count', acc.item_count + 1,
        'avg_price',  0D                        -- computed in finish
    ),
    acc -> NAMED_STRUCT(
        'subtotal',   acc.subtotal,
        'item_count', acc.item_count,
        'avg_price',  acc.subtotal / NULLIF(acc.item_count, 0)
    )
) AS order_summary
FROM orders;
```

### Conditional accumulator — count positives

```sql
SELECT AGGREGATE(
    ARRAY(-3, 5, -1, 8, 2, -6, 4),
    0,
    (acc, x) -> acc + CASE WHEN x > 0 THEN 1 ELSE 0 END
) AS positive_count;
-- Result: 4
```

### Fold over struct array

```sql
SELECT AGGREGATE(
    ARRAY(
        NAMED_STRUCT('category', 'food',  'amount', 120.0),
        NAMED_STRUCT('category', 'rent',  'amount', 800.0),
        NAMED_STRUCT('category', 'utils', 'amount',  95.0)
    ),
    0D,
    (acc, item) -> acc + item.amount,
    total -> ROUND(total, 2)
) AS monthly_expenses;
-- Result: 1015.0
```

### NULL-safe accumulator (guarding against poisoned folds)

```sql
-- Without a guard, one NULL element poisons the entire running sum
SELECT AGGREGATE(ARRAY(10, 20, NULL, 40), 0, (acc, x) -> acc + x) AS unsafe_sum;
-- Result: NULL  <- every step after the NULL element is also NULL

-- COALESCE inside the merge lambda treats NULL elements as zero
SELECT AGGREGATE(
    ARRAY(10, 20, NULL, 40), 0,
    (acc, x) -> acc + COALESCE(x, 0)
) AS safe_sum;
-- Result: 70
```

### Nested array reduction (recursive AGGREGATE)

```sql
-- Sum every element across a jagged array of arrays by recursing into AGGREGATE
SELECT AGGREGATE(
    ARRAY(ARRAY(1, 2), ARRAY(3, 4), ARRAY(5)),
    0,
    (acc, a) -> acc + AGGREGATE(a, 0, (inner_acc, x) -> inner_acc + x)
) AS nested_sum;
-- Result: 15
```

### Aggregate a struct field directly

```sql
-- No need to project s.score first — reference the field inside the merge lambda
SELECT AGGREGATE(
    ARRAY(
        NAMED_STRUCT('name', 'Alice', 'score', 90),
        NAMED_STRUCT('name', 'Bob', 'score', 85)
    ),
    0,
    (acc, s) -> acc + s.score
) AS total_score;
-- Result: 175
```

______________________________________________________________________

## :material-compare: AGGREGATE vs Built-in Equivalents

| Operation           |                     Using `AGGREGATE`                     |                 Built-in shortcut                  |
| ------------------- | :-------------------------------------------------------: | :------------------------------------------------: |
| Sum elements        |              `AGGREGATE(arr, 0, (a,x)->a+x)`              | `AGGREGATE` (no built-in `array_sum` in Spark SQL) |
| Min element         | `AGGREGATE(arr, NULL, (a,x)->IF(a IS NULL OR x<a, x, a))` |                  `ARRAY_MIN(arr)`                  |
| Max element         | `AGGREGATE(arr, NULL, (a,x)->IF(a IS NULL OR x>a, x, a))` |                  `ARRAY_MAX(arr)`                  |
| Count elements      |              `AGGREGATE(arr, 0, (a,x)->a+1)`              |                    `SIZE(arr)`                     |
| Average             |                Struct accumulator + finish                |                 — (no `array_avg`)                 |
| Custom weighted avg |                 `ZIP_WITH` + `AGGREGATE`                  |                         —                          |
| String join         |                    Concat accumulator                     |               `ARRAY_JOIN(arr, ',')`               |

!!! tip "Prefer built-ins when they exist"

    `ARRAY_MIN`, `ARRAY_MAX`, `ARRAY_JOIN`, `SIZE` are faster than equivalent
    `AGGREGATE` expressions because they are implemented as native functions.
    Use `AGGREGATE` for custom logic that has no built-in equivalent.

______________________________________________________________________

## :material-magnify: Behavior Notes

1. **NULL elements** — if an element is NULL, it is passed to the merge lambda as NULL; guard with `COALESCE(x, 0)` inside the lambda.
2. **NULL init value** — the initial accumulator can be NULL (typed); use `CAST(NULL AS INT)` to specify the type.
3. **Finish lambda is optional** — omit it when the raw accumulator is the desired result.
4. **Empty array** — `AGGREGATE` on an empty array returns `init_value` (the finish lambda is still applied to it).
5. **Performance** — `AGGREGATE` processes elements sequentially per row; it does not benefit from Spark parallelism within a single array.
6. **NULL poisoning** — because `+`, string concat, and most operators propagate `NULL`, a single `NULL` element makes every subsequent accumulator value `NULL` too — the fold never "recovers." Always `COALESCE` inside the merge lambda when the array may contain `NULL`.
