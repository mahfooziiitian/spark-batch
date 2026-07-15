# :material-sigma: ANY_VALUE

`ANY_VALUE` returns an arbitrary (non-deterministic) value from a group.
Use it when you need a representative sample from a group and any value is acceptable.

---

## :material-code-tags: Syntax

```sql
ANY_VALUE(expr [, ignoreNulls])
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `expr` | Any | Column or expression to evaluate |
| `ignoreNulls` | Boolean | When `TRUE`, skips NULL values (default: `FALSE`) |

**Returns:** Same type as `expr`

---

## :material-information-outline: Behavior

1. Returns one arbitrary value from the group — the result is **non-deterministic**.
2. When `ignoreNulls` is `FALSE` (default), the result may be NULL even if non-NULL values exist.
3. When `ignoreNulls` is `TRUE`, NULLs are skipped; returns NULL only if every value is NULL.
4. Safe to use when the column is **functionally dependent** on the GROUP BY key or when any sample value is acceptable.
5. **Not suitable** when you need a specific row (e.g., the row with the max/min of another column) — use `MIN_BY` / `MAX_BY` or window functions instead.

---

## :material-flask-outline: Practical Examples

### Basic Usage

```sql
SELECT ANY_VALUE(col) AS sample_val
FROM VALUES (10), (20), (30) AS tab(col);
-- Result: one of 10, 20, or 30 (non-deterministic)
```

### Grouped Aggregation

```sql
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
    ('East', 'Alice', 100),
    ('East', 'Bob',   200),
    ('West', 'Carol', 150),
    ('West', 'Dave',  300),
    ('East', 'Eve',    50)
AS orders(region, rep, amount);

SELECT
    region,
    ANY_VALUE(rep)    AS sample_rep,
    SUM(amount)       AS total_amount
FROM orders
GROUP BY region;
```

| region | sample_rep | total_amount |
|--------|------------|--------------|
| East | Alice *or* Bob *or* Eve | 350 |
| West | Carol *or* Dave | 450 |

### NULL Handling — Default

```sql
SELECT ANY_VALUE(col) AS sample_val
FROM VALUES (NULL), (5), (20) AS tab(col);
-- Result: could be NULL, 5, or 20
```

### NULL Handling — ignoreNulls

```sql
SELECT ANY_VALUE(col, TRUE) AS sample_val
FROM VALUES (NULL), (5), (20) AS tab(col);
-- Result: 5 or 20 (never NULL when non-NULL values exist)
```

### All NULLs

```sql
SELECT ANY_VALUE(CAST(NULL AS INT), TRUE) AS sample_val
FROM VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)) AS tab(col);
-- Result: NULL
```

### Non-Aggregated Column with GROUP BY

When a column is functionally dependent on the GROUP BY key (or any sample is acceptable),
`ANY_VALUE` avoids adding it to the GROUP BY clause:

```sql
SELECT
    dept,
    ANY_VALUE(dept_name) AS dept_name,
    AVG(salary)          AS avg_salary
FROM employees
GROUP BY dept;
```

!!! warning "Non-deterministic results"

    `ANY_VALUE` does **not** guarantee which row's value is returned.
    If you need the value associated with the min or max of another column,
    use `MIN_BY(x, y)` / `MAX_BY(x, y)` or a window function with `ROW_NUMBER`.

---

## :material-lightbulb-outline: When to Use

| Scenario | Recommended Function |
|----------|---------------------|
| Any sample value from a group | `ANY_VALUE` |
| Any non-NULL sample value | `ANY_VALUE(col, TRUE)` |
| Value at the row with max/min of another column | `MAX_BY` / `MIN_BY` |
| First value in ordered group | `FIRST` / `FIRST_VALUE` |
| Last value in ordered group | `LAST` / `LAST_VALUE` |
