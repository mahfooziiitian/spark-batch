# :material-compare: WHERE vs HAVING

Both clauses filter data, but they do so at different stages. PySpark 4.2 confirms the expected rule: `WHERE` filters rows before aggregation, while `HAVING` filters groups after aggregation.

______________________________________________________________________

### :material-animation-play: Interactive Visualization — Predicate Stage Switcher

<div id="viz-having-vs-where" class="ts-viz"></div>

Switch the same predicate between `WHERE` and `HAVING` and compare both the surviving row counts and the plan snippet. The key difference is whether the filter sits below or above `Aggregate`.

<script src="../../assets/js/querying-having-viz.js"></script>

______________________________________________________________________

## :material-sitemap: Execution Order

```mermaid
flowchart LR
    FROM --> WHERE["WHERE<br/>row filter"]
    WHERE --> GROUP["GROUP BY<br/>aggregate"]
    GROUP --> HAVING["HAVING<br/>group filter"]
    HAVING --> SELECT["SELECT / ORDER BY"]
```

______________________________________________________________________

## :material-table: Side-by-Side Comparison

| Factor                    | WHERE             | HAVING                                     |
| ------------------------- | ----------------- | ------------------------------------------ |
| When it runs              | Before `GROUP BY` | After `GROUP BY`                           |
| What it filters           | Individual rows   | Aggregated groups                          |
| Can reference raw columns | Yes               | Only grouped columns or resolved aliases   |
| Can reference aggregates  | No                | Yes                                        |
| Performance               | Often better      | Usually later and therefore more expensive |
| Best use                  | Row predicates    | Aggregate predicates                       |

______________________________________________________________________

## :material-flask-outline: Verified Plan Evidence

Spark 4.2 produced this optimized plan for a `HAVING` query:

```sql
Filter (cnt#8L >= 5)
+- Aggregate [_groupingexpression#12L], [_groupingexpression#12L AS bucket#7L, count(1) AS cnt#8L]
   +- Project [(id#9L % 2) AS _groupingexpression#12L]
      +- Range (0, 10, step=1)
```

The same style of query with `WHERE id >= 5` produced this optimized plan:

```sql
Aggregate [_groupingexpression#4L], [_groupingexpression#4L AS bucket#0L, count(1) AS cnt#1L]
+- Project [(id#2L % 2) AS _groupingexpression#4L]
   +- Filter (id#2L >= 5)
      +- Range (0, 10, step=1)
```

That is the practical proof that `HAVING` sits above `Aggregate`, while `WHERE` can be applied before grouping.

______________________________________________________________________

## :material-flask-outline: Examples

### Row predicate belongs in `WHERE`

```sql
SELECT
    region,
    SUM(amount) AS total
FROM orders
WHERE region = 'APAC'
GROUP BY region;
```

Writing the same predicate in `HAVING` is usually legal but wasteful because Spark still has to build the group first.

### Aggregate predicate requires `HAVING`

```sql
SELECT
    customer_id,
    COUNT(*) AS cnt
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 5;
```

The corresponding `WHERE COUNT(*) > 5` form fails analysis because `WHERE` cannot use aggregates.

### `WHERE` and `HAVING` together with different predicates

```sql
SELECT
    category,
    COUNT(*) AS cnt,
    SUM(amount) AS total
FROM orders
WHERE status = 'open'
GROUP BY category
HAVING SUM(amount) > 20;
```

Spark 4.2 keeps only rows with `status = 'open'`, forms per-`category` groups from those rows, then discards any group whose summed `amount` does not exceed `20`.

### Alias in `HAVING`

```sql
SELECT
    region,
    SUM(amount) AS total_revenue
FROM orders
GROUP BY region
HAVING total_revenue > 100000;
```

Spark 4.2 resolves the aggregate alias `total_revenue` inside `HAVING`.

______________________________________________________________________

## :material-speedometer: Performance Guidance

| Scenario                       | Recommendation                                    |
| ------------------------------ | ------------------------------------------------- |
| Filter by a raw column         | Use `WHERE`                                       |
| Filter by an aggregate value   | Use `HAVING`                                      |
| Both types in one query        | Split them: `WHERE` first, `HAVING` second        |
| Highly selective row predicate | Push to `WHERE` so fewer rows reach the aggregate |

!!! tip

    If a predicate does not depend on an aggregate result, push it as early as possible. That usually means `WHERE`, not `HAVING`.

______________________________________________________________________

## :material-alert-circle: Common Mistakes

| Mistake                                           | Fix                                        |
| ------------------------------------------------- | ------------------------------------------ |
| `HAVING col = 'value'` for a pure row filter      | Move it to `WHERE`                         |
| `WHERE SUM(col) > n`                              | Change it to `HAVING SUM(col) > n`         |
| `HAVING amount > 15` when `amount` is not grouped | Aggregate it or move the predicate earlier |
| Ratio test without a zero guard                   | Use `NULLIF(...)` in the denominator       |
