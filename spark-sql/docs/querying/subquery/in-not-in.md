# :material-format-list-bulleted: IN / NOT IN Subqueries

`IN` and `NOT IN` compare a value against a set. In PySpark 4.2 they are concise and optimizer-friendly, but `NOT IN` remains dangerous whenever the right-hand side can contain `NULL`.

<script src="../../assets/js/querying-subquery-viz.js"></script>

### :material-animation-play: Interactive Visualization — `IN`, `NOT IN`, and NULLs

<div id="viz-subquery-in-null-trap" class="ts-viz"></div>

Flip the NULL toggle to see the exact result change that makes `NOT IN` so easy to misuse.

______________________________________________________________________

## :material-code-tags: Core Forms

```sql
SELECT region
FROM regions
WHERE region IN (SELECT country FROM blocked_countries);
```

```sql
SELECT region
FROM regions
WHERE region NOT IN (SELECT country FROM blocked_countries);
```

```sql
SELECT *
FROM t1
WHERE (col1, col2) IN (SELECT col_a, col_b FROM t2);
```

______________________________________________________________________

## :material-information-outline: Verified Spark 4.2 Behavior

1. `IN (subquery)` is optimized as a `LeftSemi` join for common equality predicates.
2. `NOT IN (subquery)` becomes a **null-aware** anti operation. When the subquery result contains any `NULL`, every non-matching comparison becomes `UNKNOWN`, so Spark returns zero rows.
3. The same NULL trap applies to **literal lists** too: `region NOT IN ('CA', NULL)` also returns zero rows.
4. Filtering `NULL` out of the subquery fixes `NOT IN` only for non-null outer values. Outer `NULL` values still do not satisfy `NOT IN`.
5. Multi-column `IN` works in Spark 4.2.

______________________________________________________________________

## :material-flask-outline: Verified Example

Sample right-hand side:

```sql
SELECT * FROM VALUES ('CA'), (NULL) AS blocked_countries(country);
```

Sample left-hand side:

```sql
SELECT * FROM VALUES ('US'), ('CA'), ('MX'), (NULL) AS regions(region);
```

Results verified in PySpark 4.2:

| Predicate                                                                         | Returned rows      |
| --------------------------------------------------------------------------------- | ------------------ |
| `region IN (SELECT country FROM blocked_countries)`                               | `CA`               |
| `region NOT IN (SELECT country FROM blocked_countries)`                           | no rows            |
| `region NOT IN (SELECT country FROM blocked_countries WHERE country IS NOT NULL)` | `MX`, `US`         |
| `NOT EXISTS (...)` rewrite                                                        | `NULL`, `MX`, `US` |

### Safe rewrite

```sql
SELECT region
FROM regions r
WHERE NOT EXISTS (
    SELECT 1
    FROM blocked_countries bc
    WHERE bc.country = r.region
);
```

______________________________________________________________________

## :material-swap-horizontal: `NOT IN` vs `NOT EXISTS`

| Concern             | `NOT IN`                      | `NOT EXISTS`                           |
| ------------------- | ----------------------------- | -------------------------------------- |
| Inner `NULL` values | Breaks the predicate          | Safe                                   |
| Outer `NULL` values | Never returned                | Returned when no equality match exists |
| Plan shape          | Null-aware anti logic         | `LeftAnti` join                        |
| Recommended default | Only on proven non-null input | Yes                                    |

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                                                  | Recommendation         |
| --------------------------------------------------------- | ---------------------- |
| Small literal inclusion list                              | `IN (...)`             |
| Membership from another table                             | `IN` or `EXISTS`       |
| Exclusion with nullable right side                        | `NOT EXISTS`           |
| Exclusion with non-null right side and non-null left side | `NOT IN` is acceptable |
| Multi-column membership                                   | Row-value `IN`         |
