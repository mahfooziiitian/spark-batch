# Sorting & Ordering

Spark SQL provides several ways to sort query results — from simple `ORDER BY` to
window-based and partition-level ordering.

## 📌 ORDER BY — Global Sort

Sorts the entire result set. Produces a **single partition** of output.

```sql
SELECT * FROM employees ORDER BY salary DESC;

-- Multiple columns
SELECT * FROM employees ORDER BY department ASC, salary DESC;
```

## 📌 SORT BY — Per-Partition Sort

Sorts within each partition (no global ordering). Useful for optimizing downstream operations.

```sql
SELECT * FROM employees SORT BY salary DESC;
```

## 📌 DISTRIBUTE BY + SORT BY — Partition Then Sort

Controls both partitioning and within-partition ordering.

```sql
SELECT * FROM employees
DISTRIBUTE BY department
SORT BY salary DESC;
```

## 📌 CLUSTER BY — Distribute + Sort by Same Key

Shorthand for `DISTRIBUTE BY col SORT BY col`.

```sql
SELECT * FROM employees CLUSTER BY department;
```

## 🔍 NULL Ordering

```sql
-- NULLs first (default for DESC in Spark)
SELECT * FROM data ORDER BY col DESC NULLS FIRST;

-- NULLs last (default for ASC in Spark)
SELECT * FROM data ORDER BY col ASC NULLS LAST;

-- Explicit control
SELECT * FROM data ORDER BY col ASC NULLS FIRST;
```

## �� Practical Examples

### 🧱 1. Top-N Query

```sql
SELECT * FROM employees ORDER BY salary DESC LIMIT 10;
```

### 🧱 2. Ranking with Window Functions

```sql
SELECT name, department, salary,
       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
FROM employees;
```

### 🧱 3. Sort Array Elements

```sql
SELECT SORT_ARRAY(ARRAY(3, 1, 4, 1, 5)) AS sorted;
-- Result: [1, 1, 3, 4, 5]

SELECT SORT_ARRAY(ARRAY(3, 1, 4), FALSE) AS desc_sorted;
-- Result: [4, 3, 1]
```

### 🧱 4. Custom Sort with CASE

```sql
SELECT * FROM tasks
ORDER BY
  CASE priority
    WHEN 'critical' THEN 1
    WHEN 'high'     THEN 2
    WHEN 'medium'   THEN 3
    WHEN 'low'      THEN 4
    ELSE 5
  END;
```

## 🧠 Comparison

| Clause | Scope | Guarantees |
|--------|-------|-----------|
| `ORDER BY` | Global | Fully sorted output (single partition) |
| `SORT BY` | Per-partition | Sorted within each partition only |
| `DISTRIBUTE BY` | Partitioning | Controls which rows go to which partition |
| `CLUSTER BY` | Both | `DISTRIBUTE BY` + `SORT BY` same column |

> **Tip:** Use `ORDER BY` only when you need globally sorted output (e.g., `LIMIT` queries).
> For large datasets, prefer `SORT BY` or `DISTRIBUTE BY` + `SORT BY` to avoid single-partition
> bottlenecks.
