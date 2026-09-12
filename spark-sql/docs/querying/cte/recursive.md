# :material-repeat: Recursive CTEs

`WITH RECURSIVE` lets a query feed its own next iteration. In PySpark 4.2 this works out of the box, making recursive CTEs practical for sequences, hierarchies, bounded graph walks, and gap-free date spines.

### :material-animation-play: Interactive Visualization — Anchor, Iterate, Stop

<div id="viz-cte-recursive-steps" class="ts-viz"></div>

Step through the anchor row, each recursive expansion, and the point where no new rows are produced. The controls also surface the verified Spark 4.2 safeguards: default recursion-level and row limits.

______________________________________________________________________

## :material-code-tags: Syntax

```sql
WITH RECURSIVE cte_name (col1, col2, ...) AS (
    SELECT ...

    UNION ALL

    SELECT ...
    FROM cte_name
    WHERE ...
)
SELECT * FROM cte_name;
```

Spark 4.2 also accepts a per-query depth override:

```sql
WITH RECURSIVE numbers(n) MAX RECURSION LEVEL 200 AS (
    SELECT 1
    UNION ALL
    SELECT n + 1
    FROM numbers
    WHERE n < 101
)
SELECT MAX(n) FROM numbers;
```

______________________________________________________________________

## :material-information-outline: Verified Behavior in Spark 4.2

1. **Recursive CTEs are supported** — simple numeric and multi-column examples executed successfully in PySpark 4.2.
2. **ANSI mode is not required** — the same recursive query worked with `spark.sql.ansi.enabled = false`.
3. **Default recursion safeguards are configurable** — `spark.sql.cteRecursionLevelLimit` defaults to `100`, and `spark.sql.cteRecursionRowLimit` defaults to `1000000`.
4. **You can override depth per statement** — `MAX RECURSION LEVEL 200` allowed a query that exceeded the default 100-level cap.
5. **Type compatibility matters across the union** — if the anchor emits `DATE`, a recursive branch using `DATEADD(DAY, 1, dt)` must cast back to `DATE`, or use `date_add(dt, 1)`, because `DATEADD` returns a timestamp-like value in Spark 4.2.

!!! warning "Always bound recursion"

    Use a real termination condition and often a depth guard as well. When the query does
    not exhaust before the default level limit, Spark 4.2 raises
    `RECURSION_LEVEL_LIMIT_EXCEEDED`.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Integer sequence

```sql
WITH RECURSIVE nums AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1
    FROM nums
    WHERE n < 10
)
SELECT n
FROM nums
ORDER BY n;
```

### Date spine

```sql
WITH RECURSIVE date_spine AS (
    SELECT DATE '2024-01-01' AS dt
    UNION ALL
    SELECT date_add(dt, 1)
    FROM date_spine
    WHERE dt < DATE '2024-01-31'
)
SELECT dt
FROM date_spine
ORDER BY dt;
```

### Fibonacci sequence

```sql
WITH RECURSIVE fib (n, a, b) AS (
    SELECT 0, 0, 1
    UNION ALL
    SELECT n + 1, b, a + b
    FROM fib
    WHERE n < 15
)
SELECT n, a AS fibonacci_number
FROM fib
ORDER BY n;
```

### Organisational hierarchy traversal

```sql
WITH RECURSIVE org_tree AS (
    SELECT
        employee_id,
        name,
        manager_id,
        department,
        0 AS depth,
        CAST(name AS STRING) AS path
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    SELECT
        e.employee_id,
        e.name,
        e.manager_id,
        e.department,
        t.depth + 1,
        concat(t.path, ' > ', e.name)
    FROM employees AS e
    JOIN org_tree AS t
        ON e.manager_id = t.employee_id
    WHERE t.depth < 10
)
SELECT
    REPEAT('  ', depth) || name AS org_chart,
    department,
    depth
FROM org_tree
ORDER BY path;
```

### Ancestor lookup

```sql
WITH RECURSIVE ancestors AS (
    SELECT employee_id, manager_id, name, 0 AS depth
    FROM employees
    WHERE employee_id = 42

    UNION ALL

    SELECT e.employee_id, e.manager_id, e.name, a.depth + 1
    FROM employees AS e
    JOIN ancestors AS a
        ON e.employee_id = a.manager_id
    WHERE a.depth < 20
)
SELECT employee_id, name, depth AS levels_above
FROM ancestors
ORDER BY depth;
```

______________________________________________________________________

## :material-lightbulb-outline: When to Use Recursive CTEs

| Scenario                             | Pattern                       |
| ------------------------------------ | ----------------------------- |
| Generate integers or small sequences | Anchor row + increment        |
| Build a date spine                   | Anchor date + `date_add`      |
| Walk an org chart or category tree   | Anchor at root, join children |
| Find ancestors                       | Anchor at leaf, join parents  |
| Traverse a bounded graph             | Recursive join + depth guard  |

!!! tip "Prefer a plain window function when recursion is unnecessary"

    Recursive CTEs are for iterative structure. For running totals, ranking, and lead/lag
    style problems, window functions are simpler and usually faster.
