# :material-code-equal: Join Expressions in Spark SQL

A join expression tells Spark how two relations match. In Spark 4.2, the difference between `ON`, `USING`, `NATURAL JOIN`, and comma syntax affects both the result schema and the physical strategy Catalyst can choose.

### :material-animation-play: Interactive Visualization — Join Syntax Resolver

<div id="viz-join-expression-core" class="ts-viz"></div>

Toggle between `ON`, `USING`, `NATURAL JOIN`, and comma syntax to compare resolved columns and planner behavior.

<script src="../../assets/js/querying-joins-core-viz.js"></script>

______________________________________________________________________

## :material-table: Verified Syntax Differences

| Syntax                       | What Spark 4.2 resolves                                            | Verified behavior                                                                                                                |
| ---------------------------- | ------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| `JOIN ... ON condition`      | Uses the exact boolean condition you provide                       | Keeps both sides' join columns unless you project them away                                                                      |
| `JOIN ... USING (col1, ...)` | Builds equality predicates on same-named columns listed in `USING` | Collapses only the `USING` columns into one output copy                                                                          |
| `NATURAL JOIN`               | Builds equality predicates on every shared column name             | Collapses all shared columns; output order is common columns, left-only columns, then right-only columns                         |
| `FROM a, b`                  | Comma join syntax                                                  | With no predicate it is a cartesian product; with an equi predicate in `WHERE`, Catalyst can optimize it into an inner equi join |

!!! warning "Why explicit syntax is safer"

    `NATURAL JOIN` and comma syntax depend on column names or later filters. `JOIN ... ON` is usually the clearest and least surprising form for production SQL.

______________________________________________________________________

## :material-link-variant: `ON` Clause

Use `ON` when the match logic is explicit or the column names differ.

```sql
SELECT
    c.id,
    o.order_id,
    o.amount
FROM customers AS c
JOIN orders AS o
    ON c.id = o.customer_id;
```

Multiple conditions are fine:

```sql
SELECT *
FROM employees AS e
JOIN departments AS d
    ON e.dept_id = d.id
   AND e.location = d.location;
```

PySpark 4.2 still planned a sort-merge equi join when both sides used the same deterministic function on the join key, for example:

```sql
SELECT *
FROM a
JOIN b
    ON lower(CAST(a.id AS STRING)) = lower(CAST(b.id AS STRING));
```

______________________________________________________________________

## :material-table-column: `USING`

Use `USING` when the join keys have the same name on both sides.

```sql
SELECT *
FROM customers
JOIN orders
USING (customer_id);
```

Verified behavior in PySpark 4.2:

- Spark expands `USING (customer_id)` into an equality join on that column.
- The output contains one `customer_id` column, not two.
- Other same-named columns are **not** automatically collapsed unless they are also listed in `USING`.

That last point matters. In a test query `SELECT * FROM a JOIN b USING (id)`, Spark returned one `id` column but still returned duplicate `grp` and `val` columns because they were not named in `USING`.

______________________________________________________________________

## :material-family-tree: `NATURAL JOIN`

`NATURAL JOIN` is shorthand for "join on every shared column name."

```sql
SELECT *
FROM nat_l
NATURAL JOIN nat_r;
```

Verified behavior in PySpark 4.2:

- Spark matched on **all** common columns, not just one.
- Shared columns appeared once in the output.
- Output column order was: common columns first, then left-only columns, then right-only columns.

In the test data, both tables shared `id` and `common`. Only the row where **both** values matched survived, so `(id = 1, common = 'x')` joined, while `(id = 1, common = 'z')` did not.

!!! note "Schema drift risk"

    Because `NATURAL JOIN` follows shared column names automatically, adding a same-named column later can silently change the join condition.

______________________________________________________________________

## :material-arrow-expand-horizontal: Range and Other Non-Equi Predicates

Range predicates are valid `ON` clauses:

```sql
SELECT *
FROM transactions AS t
JOIN tax_slabs AS s
    ON t.amount BETWEEN s.min_amount AND s.max_amount;
```

```sql
SELECT *
FROM points AS p
JOIN ranges AS r
    ON p.value >= r.start
   AND p.value < r.end;
```

In the PySpark 4.2 verification run, a pure range join with no usable equi key planned as `CartesianProduct` in `EXPLAIN FORMATTED`.

______________________________________________________________________

## :material-call-split: `OR` Conditions

```sql
SELECT *
FROM flights AS f
JOIN routes AS r
    ON f.src = r.src
    OR f.dest = r.dest;
```

A disjunctive predicate can be valid SQL, but it is harder for Spark to optimize as a hash or sort-merge equi join. In the verification run, an `OR` predicate planned as `CartesianProduct`.

______________________________________________________________________

## :material-grid: Cross and Comma Joins

Explicit cross join:

```sql
SELECT *
FROM products AS p
CROSS JOIN dates AS d;
```

Implicit comma join:

```sql
SELECT *
FROM a, b;
```

Comma syntax with a later predicate:

```sql
SELECT *
FROM a, b
WHERE a.id = b.id;
```

Verified behavior in PySpark 4.2:

- `FROM a, b` planned as `CartesianProduct`.
- `FROM a, b WHERE a.id = b.id` was optimized back into an inner equi join plan (`SortMergeJoin` in the test run).

______________________________________________________________________

## :material-lightbulb-outline: Practical Guidance

- Prefer `JOIN ... ON` for explicitness.
- Use `USING` when you want a single output copy of specific same-named join keys.
- Use `NATURAL JOIN` sparingly because schema changes alter its semantics.
- Treat range and `OR` joins as more expensive unless you can keep an equi key in the predicate.
- Confirm the chosen operator with `EXPLAIN FORMATTED` instead of assuming the planner found an equi join.
