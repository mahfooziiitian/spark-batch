# :material-set-none: Set Operators

Set operators combine whole result sets. Spark SQL 4.2 supports both deduplicating and duplicate-preserving variants, so it helps to think in terms of sets versus multisets.

## :material-animation-play: Interactive Visualization — Set vs Multiset Results

<div id="viz-operator-set-multiset" class="ts-viz"></div>

Switch operators to compare how Spark 4.2 handles duplicates when the left and right inputs contain repeated rows.

<script src="../../../assets/js/querying-operator-viz.js"></script>

______________________________________________________________________

## :material-code-tags: Syntax

```sql
query_a UNION query_b
query_a UNION ALL query_b
query_a INTERSECT query_b
query_a INTERSECT ALL query_b
query_a EXCEPT query_b
query_a EXCEPT ALL query_b
```

Rules verified in PySpark 4.2:

- Both sides must return the same number of columns.
- Column types must be compatible or Spark raises an analysis error.
- Output column names come from the first query.
- `MINUS` is accepted as an alias for `EXCEPT` in Spark SQL 4.2.

______________________________________________________________________

## :material-table: Verified Duplicate Semantics

These outcomes were verified with small duplicate-heavy inputs chosen to make multiplicity visible.

| Expression checked in PySpark 4.2                     | Result                    |
| ----------------------------------------------------- | ------------------------- |
| `SELECT 1 UNION SELECT 1`                             | one row: `1`              |
| `SELECT 1 UNION ALL SELECT 1`                         | two rows: `1`, `1`        |
| `INTERSECT` on `(1), (1), (2)` and `(1), (1), (3)`    | one row: `1`              |
| `INTERSECT ALL` on those inputs                       | two rows: `1`, `1`        |
| `EXCEPT` on `(1), (1), (2)` minus `(1), (3)`          | one row: `2`              |
| `EXCEPT ALL` on `(1), (1), (1), (2)` minus `(1), (3)` | three rows: `1`, `1`, `2` |

`INTERSECT ALL` was also verified with `NULL` values on both sides, and matching `NULL` rows were preserved in the result.

______________________________________________________________________

## :material-information-outline: Compatibility Rules

### Same column count is mandatory

```sql
SELECT 1
UNION
SELECT 1, 2;
-- NUM_COLUMNS_MISMATCH in Spark 4.2
```

### Types must be compatible

```sql
SELECT 1 AS x
UNION
SELECT MAP(1, 2) AS x;
-- INCOMPATIBLE_COLUMN_TYPE in Spark 4.2
```

### Compatible types are coerced

```sql
SELECT typeof(x) AS t, x
FROM (
    SELECT 1 AS x
    UNION ALL
    SELECT 2.5 AS x
) AS t
ORDER BY x;
-- Verified result type: decimal(11,1)
```

### Column names come from the first query

```sql
SELECT *
FROM (
    SELECT 1 AS left_name
    UNION ALL
    SELECT 2 AS right_name
) AS t;
-- Output column name: left_name
```

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Append without deduplication

```sql
SELECT order_id, customer_id, amount
FROM orders_current
UNION ALL
SELECT order_id, customer_id, amount
FROM orders_history;
```

### Deduplicate a combined stream

```sql
SELECT customer_id FROM crm_customers
UNION
SELECT customer_id FROM ecommerce_customers;
```

### Find common rows including duplicate counts

```sql
SELECT order_id, status FROM pending_orders
INTERSECT ALL
SELECT order_id, status FROM processed_orders;
```

### Subtract duplicate counts, not just distinct values

```sql
SELECT item_id FROM basket_a
EXCEPT ALL
SELECT item_id FROM basket_b;
```

### `MINUS` alias

```sql
SELECT 1 AS x
MINUS
SELECT 2 AS x;
-- Verified in PySpark 4.2: returns 1
```

______________________________________________________________________

## :material-set-center: Set-Based Business Problems — Customer Segments as Set Algebra

Reframing customer analytics as set algebra makes the SQL fall out directly:

```text
A = customers who purchased
B = customers who logged in
C = customers who subscribed
```

| Set operation | Business question                        | SQL patterns that compute it                                                              |
| ------------- | ---------------------------------------- | ----------------------------------------------------------------------------------------- |
| `A ∩ B`       | Purchased **and** logged in              | `INTERSECT`, correlated `EXISTS`, `LEFT SEMI JOIN`                                        |
| `A − B`       | Purchased but **never** logged in        | `EXCEPT`, correlated `NOT EXISTS`, `LEFT ANTI JOIN` (avoid `NOT IN` — see the trap below) |
| `A ∪ B`       | Purchased **or** logged in (or both)     | `UNION` (dedupes) / `UNION ALL` (keeps both copies if a customer appears in both)         |
| `A ∩ B ∩ C`   | Purchased, logged in, **and** subscribed | Chain `INTERSECT` twice, or stack `LEFT SEMI JOIN` against `B` then `C`                   |

Three ways to express `A ∩ B` (`purchased` ∩ `logged_in`) look interchangeable —
they are not. Verified side by side on Spark 4.2:

```sql
-- 1. Set operator
SELECT customer_id FROM purchased INTERSECT SELECT customer_id FROM logged_in;

-- 2. Correlated EXISTS
SELECT customer_id FROM purchased p
WHERE EXISTS (SELECT 1 FROM logged_in l WHERE l.customer_id = p.customer_id);

-- 3. Explicit semi join
SELECT p.customer_id FROM purchased p LEFT SEMI JOIN logged_in l ON p.customer_id = l.customer_id;
```

All three compile to the **same underlying Spark join strategy** —
`BroadcastHashJoin`/`SortMergeJoin` with `Join type: LeftSemi` — confirmed via
`EXPLAIN FORMATTED` on all three forms. `EXCEPT`, `NOT EXISTS`, and `LEFT ANTI JOIN` likewise all converge on `Join type: LeftAnti`. Semi/anti joins are not a
separate execution family from `INTERSECT`/`EXCEPT` — they *are* how Spark
implements `INTERSECT`/`EXCEPT` internally, plus one extra step (next section).

### The hidden difference: row-multiplicity semantics

`INTERSECT`/`EXCEPT` add a deduplicating `HashAggregate` **after** the semi/anti
join; `EXISTS`/`NOT EXISTS`/`LEFT SEMI JOIN`/`LEFT ANTI JOIN` do not — they pass
through however many matching rows existed on the left side. Verified with
`purchased` containing three `customer_id = 2` rows and one `customer_id = 3` row:

```sql
SELECT customer_id FROM purchased INTERSECT SELECT customer_id FROM logged_in;
-- 2 rows: customer_id 2, 3  (deduplicated)

SELECT p.customer_id FROM purchased p LEFT SEMI JOIN logged_in l ON p.customer_id = l.customer_id;
-- 4 rows: 2, 2, 2, 3  (all three purchases by customer 2 are preserved)
```

!!! danger "Don't substitute `LEFT SEMI JOIN` for `INTERSECT` (or vice versa) without checking row counts"

    If `A` has duplicate keys (multiple orders per customer, multiple events per
    session), `INTERSECT`/`EXCEPT` silently collapse them to one row per key while
    `LEFT SEMI JOIN`/`LEFT ANTI JOIN`/`EXISTS`/`NOT EXISTS` silently preserve every
    matching row. Both are "correct" for their own semantics — the bug is assuming
    they're interchangeable. If you need one distinct row per customer regardless
    of which pattern you pick, dedupe explicitly (`SELECT DISTINCT` or
    `GROUP BY`) rather than relying on the pattern choice to do it for you.

### The other hidden difference: `NOT IN` and `NULL`

`A − B` has a third candidate — `NOT IN` — and it is the one to avoid. Verified
on Spark 4.2 with a single `NULL` customer_id in the `logged_in_with_null` table:

```sql
SELECT customer_id FROM purchased
WHERE customer_id NOT IN (SELECT customer_id FROM logged_in_with_null);
-- Returns ZERO rows — even for purchasers who were never in logged_in_with_null at all

SELECT customer_id FROM purchased p
WHERE NOT EXISTS (SELECT 1 FROM logged_in_with_null l WHERE l.customer_id = p.customer_id);
-- Correctly returns the purchasers who never logged in

SELECT p.customer_id FROM purchased p
LEFT ANTI JOIN logged_in_with_null l ON p.customer_id = l.customer_id;
-- Also correct -- LEFT ANTI JOIN is NULL-safe by construction
```

`x NOT IN (a, b, NULL)` evaluates as `x != a AND x != b AND x != NULL` — and any
comparison against `NULL` is `UNKNOWN`, which makes the entire `AND` chain
`UNKNOWN` (never `TRUE`) for every row, regardless of `x`. A single stray `NULL`
in the subquery silently empties the whole result — no error, no warning. `NOT EXISTS` and `LEFT ANTI JOIN` don't have this failure mode because they test
row *existence*, not per-value equality against a set that might contain
`NULL`. See [`IN` / `NOT IN` Subqueries](../subquery/in-not-in.md) for the
full breakdown and [`NOT EXISTS`](../subquery/exists.md) /
[Left Anti Join](../joins/types/left-anti.md) for the NULL-safe alternatives.

### Decision table

| Need                                              | Prefer                                       | Avoid                                     |
| ------------------------------------------------- | -------------------------------------------- | ----------------------------------------- |
| `A ∩ B`, one row per distinct key                 | `INTERSECT`                                  | —                                         |
| `A ∩ B`, preserve every matching row from `A`     | `LEFT SEMI JOIN` / `EXISTS`                  | `INTERSECT` (it will collapse duplicates) |
| `A − B`, one row per distinct key                 | `EXCEPT`                                     | `NOT IN` if `B` can contain `NULL`        |
| `A − B`, preserve every non-matching row from `A` | `LEFT ANTI JOIN` / `NOT EXISTS`              | `NOT IN` if `B` can contain `NULL`        |
| `A ∪ B`, distinct union                           | `UNION`                                      | —                                         |
| `A ∪ B`, keep both copies if present in both      | `UNION ALL`                                  | —                                         |
| `A ∩ B ∩ C`                                       | Chain `INTERSECT` / stacked `LEFT SEMI JOIN` | —                                         |

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                               | Recommended operator |
| -------------------------------------- | -------------------- |
| Append rows exactly as-is              | `UNION ALL`          |
| Combine and deduplicate                | `UNION`              |
| Find common distinct rows              | `INTERSECT`          |
| Find common rows with duplicate counts | `INTERSECT ALL`      |
| Subtract distinct rows                 | `EXCEPT`             |
| Subtract row multiplicity              | `EXCEPT ALL`         |

!!! tip

    Prefer `UNION ALL` when deduplication is not required. It avoids the extra distinct step that `UNION` performs.
