# :material-check-circle-outline: EXISTS / NOT EXISTS Subqueries

`EXISTS` keeps an outer row when the subquery returns at least one match. `NOT EXISTS` keeps it when the subquery returns none. In Spark 4.2 they are the safest subquery forms for inclusion and exclusion checks.

<script src="../../assets/js/querying-subquery-viz.js"></script>

### :material-animation-play: Interactive Visualization — Semi Join vs Anti Join

<div id="viz-subquery-exists-semi-anti" class="ts-viz"></div>

Compare how `EXISTS` and `NOT EXISTS` react to matching rows and to a `NULL` present on the inner side.

______________________________________________________________________

## :material-code-tags: Core Forms

```sql
SELECT name
FROM customers c
WHERE EXISTS (
    SELECT 1
    FROM customer_orders o
    WHERE o.customer_id = c.customer_id
);
```

```sql
SELECT name
FROM customers c
WHERE NOT EXISTS (
    SELECT 1
    FROM customer_orders o
    WHERE o.customer_id = c.customer_id
);
```

______________________________________________________________________

## :material-information-outline: Verified Spark 4.2 Behavior

1. Equality-based `EXISTS` is optimized to a `LeftSemi` join.
2. Equality-based `NOT EXISTS` is optimized to a `LeftAnti` join.
3. `NOT EXISTS` is **NULL-safe** with respect to inner rows: an unrelated `NULL` on the right does not poison the whole predicate.
4. The subquery `SELECT` list is irrelevant for existence testing; `SELECT 1` is just convention.
5. For non-equality correlation, Spark may still decorrelate the query but use a heavier join such as `BroadcastNestedLoopJoin`.

______________________________________________________________________

## :material-flask-outline: Verified Results

Using customers `Alice`, `Bob`, `Charlie`, `Dave`, and `Eve`, plus orders only for customer ids `1` and `2`:

```sql
SELECT name
FROM customers c
WHERE EXISTS (
    SELECT 1
    FROM customer_orders o
    WHERE o.customer_id = c.customer_id
)
ORDER BY name;
```

Returned `Alice` and `Bob`.

```sql
SELECT name
FROM customers c
WHERE NOT EXISTS (
    SELECT 1
    FROM customer_orders o
    WHERE o.customer_id = c.customer_id
)
ORDER BY name;
```

Returned `Charlie`, `Dave`, and `Eve`.

### NULL-safe exclusion example

```sql
SELECT region
FROM regions r
WHERE NOT EXISTS (
    SELECT 1
    FROM blocked_countries bc
    WHERE bc.country = r.region
)
ORDER BY region;
```

With `blocked_countries = ('CA'), (NULL)`, Spark 4.2 returned `NULL`, `MX`, and `US`.

______________________________________________________________________

## :material-swap-horizontal: `EXISTS` Compared with `IN`

| Question                                    | `EXISTS`          | `IN`                               |
| ------------------------------------------- | ----------------- | ---------------------------------- |
| Need only to know whether any match exists? | Best fit          | Works                              |
| Inner duplicates matter?                    | No                | No                                 |
| Inner `NULL` hurts exclusion form?          | No (`NOT EXISTS`) | Yes (`NOT IN`)                     |
| Typical plan                                | Semi/anti join    | Semi join or null-aware anti logic |

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                       | Recommendation                        |
| ------------------------------ | ------------------------------------- |
| Has at least one related row   | `EXISTS`                              |
| Has no related row             | `NOT EXISTS`                          |
| Replace unsafe `NOT IN`        | `NOT EXISTS`                          |
| Correlated non-equality filter | Use `EXPLAIN`; may become nested loop |
