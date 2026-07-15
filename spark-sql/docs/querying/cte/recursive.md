# :material-repeat: Recursive CTEs

`WITH RECURSIVE` allows a CTE to reference itself, producing rows iteratively until a
termination condition is met. Spark SQL 3.5+ supports recursive CTEs for generating
sequences, traversing trees, and walking graphs.

---

## :material-code-tags: Syntax

```sql
WITH RECURSIVE cte_name (col1, col2, ...) AS (
    -- Anchor: the non-recursive base case
    SELECT ...

    UNION ALL

    -- Recursive step: references cte_name
    SELECT ...
    FROM   cte_name
    WHERE  <termination_condition>
)
SELECT * FROM cte_name;
```

| Part | Role |
|------|------|
| **Anchor** | The starting rows — evaluated once |
| **`UNION ALL`** | Combines anchor rows and each recursive step |
| **Recursive step** | References `cte_name`; uses rows from the previous iteration |
| **Termination condition** | `WHERE` clause in the recursive step; when no rows are produced, recursion stops |

!!! warning "Spark 3.5+ only"
    `WITH RECURSIVE` requires Spark 3.5 or later and
    `spark.sql.ansi.enabled = true` (or Databricks Runtime 14.0+).
    Earlier versions raise `AnalysisException: Recursive CTE is not supported`.

!!! warning "Infinite loops"
    Always include a termination condition in the recursive step.
    Add a `depth` counter and guard with `WHERE depth < N` as a safety net.

---

## :material-flask-outline: Practical Examples

### Integer sequence

```sql
WITH RECURSIVE nums AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM nums WHERE n < 10
)
SELECT n FROM nums;
-- Result: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
```

### Date spine (one row per day)

```sql
WITH RECURSIVE date_spine AS (
    SELECT DATE('2024-01-01') AS dt
    UNION ALL
    SELECT DATEADD(DAY, 1, dt)
    FROM   date_spine
    WHERE  dt < DATE('2024-01-31')
)
SELECT dt FROM date_spine;
-- Result: 2024-01-01, 2024-01-02, ..., 2024-01-31
```

**Use case:** left-join against event data to produce a zero-gap time series.

```sql
WITH RECURSIVE date_spine AS (
    SELECT DATE('2024-01-01') AS dt
    UNION ALL
    SELECT DATEADD(DAY, 1, dt) FROM date_spine WHERE dt < DATE('2024-01-31')
)
SELECT
    ds.dt                       AS order_date,
    COALESCE(s.total, 0)        AS daily_total
FROM date_spine AS ds
LEFT JOIN (
    SELECT order_date, SUM(amount) AS total
    FROM orders
    GROUP BY order_date
) AS s ON ds.dt = s.order_date
ORDER BY ds.dt;
```

### Fibonacci sequence

```sql
WITH RECURSIVE fib (n, a, b) AS (
    SELECT 0, 0, 1
    UNION ALL
    SELECT n + 1, b, a + b
    FROM   fib
    WHERE  n < 15
)
SELECT n, a AS fibonacci_number FROM fib;
-- Result: 0→0, 1→1, 2→1, 3→2, 4→3, 5→5, ... 15→610
```

### Organisational hierarchy traversal (top-down)

```sql
-- employees(employee_id, name, manager_id, department)
WITH RECURSIVE org_tree AS (
    -- Anchor: CEO (no manager)
    SELECT
        employee_id,
        name,
        manager_id,
        department,
        0           AS depth,
        CAST(name AS STRING) AS path
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive: direct reports of the previous level
    SELECT
        e.employee_id,
        e.name,
        e.manager_id,
        e.department,
        t.depth + 1,
        concat(t.path, ' > ', e.name)
    FROM employees AS e
    JOIN org_tree  AS t ON e.manager_id = t.employee_id
    WHERE t.depth < 10          -- safety limit
)
SELECT
    REPEAT('  ', depth) || name  AS org_chart,
    department,
    depth
FROM org_tree
ORDER BY path;
```

### Ancestor path (bottom-up)

```sql
-- Find all ancestors of employee_id = 42
WITH RECURSIVE ancestors AS (
    -- Anchor: the target employee
    SELECT employee_id, manager_id, name, 0 AS depth
    FROM employees
    WHERE employee_id = 42

    UNION ALL

    -- Recursive: each manager
    SELECT e.employee_id, e.manager_id, e.name, a.depth + 1
    FROM employees AS e
    JOIN ancestors AS a ON e.employee_id = a.manager_id
    WHERE a.depth < 20
)
SELECT employee_id, name, depth AS levels_above
FROM ancestors
ORDER BY depth;
```

### Bill of materials (multi-level parts explosion)

```sql
-- parts(part_id, name, parent_part_id, quantity_per_unit)
WITH RECURSIVE bom AS (
    -- Anchor: the top-level assembly
    SELECT
        part_id,
        name,
        parent_part_id,
        quantity_per_unit,
        quantity_per_unit AS total_quantity,
        0 AS depth
    FROM parts
    WHERE part_id = 'BIKE-001'

    UNION ALL

    -- Recursive: sub-components
    SELECT
        p.part_id,
        p.name,
        p.parent_part_id,
        p.quantity_per_unit,
        b.total_quantity * p.quantity_per_unit,
        b.depth + 1
    FROM parts  AS p
    JOIN bom    AS b ON p.parent_part_id = b.part_id
    WHERE b.depth < 10
)
SELECT
    REPEAT('  ', depth) || name AS component,
    total_quantity,
    depth
FROM bom
ORDER BY depth, part_id;
```

### Cumulative running total with recursion (illustrative)

```sql
-- Better done with window functions; shown here for illustration only
WITH RECURSIVE running AS (
    SELECT
        order_id,
        amount,
        amount AS running_total,
        ROW_NUMBER() OVER (ORDER BY order_id) AS rn
    FROM (SELECT order_id, amount FROM orders ORDER BY order_id LIMIT 1)

    UNION ALL

    SELECT
        o.order_id,
        o.amount,
        r.running_total + o.amount,
        r.rn + 1
    FROM orders AS o
    JOIN running AS r ON o.order_id = (
        SELECT order_id FROM orders ORDER BY order_id
        LIMIT 1 OFFSET r.rn
    )
    WHERE r.rn < 20
)
SELECT order_id, amount, running_total FROM running;
```

!!! tip "Use window functions instead for running totals"
    `SUM(amount) OVER (ORDER BY order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
    is simpler, faster, and not limited to small row counts.

---

## :material-lightbulb-outline: When to Use Recursive CTEs

| Scenario | Pattern |
|----------|---------|
| Generate a date spine with no gaps | `WITH RECURSIVE` date series |
| Integer range for cross-join | `WITH RECURSIVE` integer sequence |
| Top-down org chart / category tree | Anchor at root, join children |
| Bottom-up ancestor lookup | Anchor at leaf, join parent |
| Bill of materials explosion | Multi-level parts join |
| Graph shortest path (small graphs) | Recursive BFS with depth guard |
