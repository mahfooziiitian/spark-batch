# :material-table-pivot: Conditional Aggregation

Aggregate data conditionally — computing separate totals, counts, or averages per category without `PIVOT` or multiple subqueries, using `SUM(CASE WHEN ...)` and `COUNT(IF(...))`.

---

## :material-toy-brick: Sample Data

```sql
-- sales — transactions across regions and product categories
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('alice',  'APAC',  'electronics', 1200.00, DATE '2024-01-15'),
  ('bob',    'EMEA',  'clothing',      89.50, DATE '2024-01-22'),
  ('alice',  'APAC',  'books',         34.99, DATE '2024-02-03'),
  ('carol',  'APAC',  'electronics',  799.00, DATE '2024-02-14'),
  ('bob',    'EMEA',  'electronics',  249.00, DATE '2024-03-01'),
  ('alice',  'APAC',  'clothing',     125.00, DATE '2024-03-10'),
  ('carol',  'EMEA',  'books',         19.99, DATE '2024-04-05'),
  ('dave',   'AMER',  'electronics',  599.00, DATE '2024-04-18'),
  ('alice',  'APAC',  'electronics',  349.00, DATE '2024-05-02'),
  ('bob',    'EMEA',  'clothing',      67.00, DATE '2024-05-20'),
  ('dave',   'AMER',  'books',         45.00, DATE '2024-05-25'),
  ('carol',  'AMER',  'electronics',  899.00, DATE '2024-06-01')
AS t(customer, region, category, amount, sale_date);
```

| customer | region | category | amount | sale_date |
|---------|--------|---------|-------|----------|
| alice | APAC | electronics | 1200.00 | 2024-01-15 |
| bob | EMEA | clothing | 89.50 | 2024-01-22 |
| carol | APAC | electronics | 799.00 | 2024-02-14 |
| dave | AMER | electronics | 599.00 | 2024-04-18 |
| … | … | … | … | … |

---

## :material-numeric-1-circle: Pattern 1 — Revenue per category as separate columns (manual pivot)

```sql
SELECT
    region,
    ROUND(SUM(CASE WHEN category = 'electronics' THEN amount ELSE 0 END), 2) AS electronics_revenue,
    ROUND(SUM(CASE WHEN category = 'clothing'    THEN amount ELSE 0 END), 2) AS clothing_revenue,
    ROUND(SUM(CASE WHEN category = 'books'       THEN amount ELSE 0 END), 2) AS books_revenue,
    ROUND(SUM(amount), 2)                                                     AS total_revenue
FROM sales
GROUP BY region
ORDER BY total_revenue DESC;
-- Result:
-- region | electronics_revenue | clothing_revenue | books_revenue | total_revenue
-- -------|---------------------|------------------|---------------|---------------
-- APAC   | 2348.00             | 125.00           |  34.99        | 2507.99
-- AMER   | 1498.00             |   0.00           |  45.00        | 1543.00
-- EMEA   |  249.00             | 156.50           |  19.99        |  425.49
```

---

## :material-numeric-2-circle: Pattern 2 — Transaction count per category per region

```sql
SELECT
    region,
    COUNT(IF(category = 'electronics', 1, NULL)) AS electronics_orders,
    COUNT(IF(category = 'clothing',    1, NULL)) AS clothing_orders,
    COUNT(IF(category = 'books',       1, NULL)) AS books_orders,
    COUNT(*)                                      AS total_orders
FROM sales
GROUP BY region
ORDER BY region;
-- Result:
-- region | electronics_orders | clothing_orders | books_orders | total_orders
-- -------|--------------------|-----------------|--------------|-------------
-- AMER   | 2                  | 0               | 1            | 3
-- APAC   | 3                  | 1               | 1            | 5
-- EMEA   | 1                  | 2               | 1            | 4
```

---

## :material-numeric-3-circle: Pattern 3 — Percentage share per category

```sql
SELECT
    region,
    ROUND(SUM(CASE WHEN category = 'electronics' THEN amount END) / SUM(amount) * 100, 1) AS electronics_pct,
    ROUND(SUM(CASE WHEN category = 'clothing'    THEN amount END) / SUM(amount) * 100, 1) AS clothing_pct,
    ROUND(SUM(CASE WHEN category = 'books'       THEN amount END) / SUM(amount) * 100, 1) AS books_pct
FROM sales
GROUP BY region
ORDER BY region;
-- Result:
-- region | electronics_pct | clothing_pct | books_pct
-- -------|-----------------|--------------|----------
-- AMER   | 97.1            |  0.0         |  2.9
-- APAC   | 93.6            |  5.0         |  1.4
-- EMEA   | 58.5            | 36.8         |  4.7
```

---

## :material-numeric-4-circle: Pattern 4 — Conditional average (exclude zeros from denominator)

Using `SUM / NULLIF(COUNT, 0)` avoids division-by-zero and correctly excludes non-matching rows from the average.

```sql
SELECT
    region,
    -- Average electronics ticket (only electronics sales in denominator)
    ROUND(
        SUM(CASE WHEN category = 'electronics' THEN amount END)
        / NULLIF(COUNT(CASE WHEN category = 'electronics' THEN 1 END), 0),
    2) AS avg_electronics_ticket,
    -- Average clothing ticket
    ROUND(
        SUM(CASE WHEN category = 'clothing' THEN amount END)
        / NULLIF(COUNT(CASE WHEN category = 'clothing' THEN 1 END), 0),
    2) AS avg_clothing_ticket
FROM sales
GROUP BY region
ORDER BY region;
-- Result:
-- region | avg_electronics_ticket | avg_clothing_ticket
-- -------|------------------------|--------------------
-- AMER   | 749.00                 | NULL
-- APAC   | 782.67                 | 125.00
-- EMEA   | 249.00                 |  78.25
```

---

## :material-numeric-5-circle: Pattern 5 — Multi-period comparison (H1 vs H2)

```sql
SELECT
    customer,
    ROUND(SUM(CASE WHEN MONTH(sale_date) BETWEEN 1 AND 3 THEN amount ELSE 0 END), 2) AS q1_revenue,
    ROUND(SUM(CASE WHEN MONTH(sale_date) BETWEEN 4 AND 6 THEN amount ELSE 0 END), 2) AS q2_revenue,
    ROUND(SUM(amount), 2)                                                             AS total_revenue,
    ROUND(
        (SUM(CASE WHEN MONTH(sale_date) BETWEEN 4 AND 6 THEN amount END)
         - SUM(CASE WHEN MONTH(sale_date) BETWEEN 1 AND 3 THEN amount END))
        / NULLIF(SUM(CASE WHEN MONTH(sale_date) BETWEEN 1 AND 3 THEN amount END), 0) * 100,
    1)                                                                                AS q2_vs_q1_growth_pct
FROM sales
GROUP BY customer
ORDER BY total_revenue DESC;
-- Result:
-- customer | q1_revenue | q2_revenue | total_revenue | q2_vs_q1_growth_pct
-- ---------|------------|------------|---------------|--------------------
-- alice    | 1359.99    |  349.00    | 1708.99       | -74.3
-- carol    | 799.00     |  918.99    | 1717.99       |  15.0
-- dave     |   0.00     |  644.00    |  644.00       |  NULL
-- bob      |  338.50    |   67.00    |  405.50       | -80.2
```

---

## :material-numeric-6-circle: Pattern 6 — Boolean flags per condition

```sql
SELECT
    customer,
    MAX(CASE WHEN category = 'electronics' THEN 1 ELSE 0 END) AS bought_electronics,
    MAX(CASE WHEN category = 'clothing'    THEN 1 ELSE 0 END) AS bought_clothing,
    MAX(CASE WHEN category = 'books'       THEN 1 ELSE 0 END) AS bought_books,
    COUNT(DISTINCT category)                                   AS category_count
FROM sales
GROUP BY customer
ORDER BY customer;
-- Result:
-- customer | bought_electronics | bought_clothing | bought_books | category_count
-- ---------|--------------------|-----------------|--------------|--------------
-- alice    | 1                  | 1               | 1            | 3
-- bob      | 1                  | 1               | 0            | 2
-- carol    | 1                  | 0               | 1            | 2
-- dave     | 1                  | 0               | 1            | 2
```

---

## :material-swap-horizontal: CASE vs IF vs FILTER

| Expression | Syntax | Use when |
|-----------|--------|----------|
| `SUM(CASE WHEN cond THEN val END)` | ANSI SQL, portable | Sum a value only for matching rows |
| `COUNT(IF(cond, 1, NULL))` | Shorter, Spark-specific | Count matching rows |
| `SUM(amount) FILTER (WHERE cond)` | SQL standard extension | Cleanest syntax — Spark 3.0+ |
| `COUNT(DISTINCT col)` | No condition needed | Distinct count across all rows |

```sql
-- FILTER syntax (cleanest for simple conditions)
SELECT
    region,
    SUM(amount) FILTER (WHERE category = 'electronics') AS electronics_revenue,
    SUM(amount) FILTER (WHERE category = 'clothing')    AS clothing_revenue,
    SUM(amount) FILTER (WHERE category = 'books')       AS books_revenue
FROM sales
GROUP BY region
ORDER BY region;
-- Identical result to Pattern 1 — SUM(CASE WHEN ...)
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Pattern |
|----------|---------|
| Pivot N known categories into columns | `SUM(CASE WHEN cat = 'X' THEN val END)` |
| Count occurrences per category | `COUNT(IF(cat = 'X', 1, NULL))` |
| Percentage share per category | `SUM(IF) / SUM(total) * 100` |
| Average excluding non-matching rows | `SUM(IF) / NULLIF(COUNT(IF), 0)` |
| Period comparison in one row | `SUM(CASE WHEN period = 'H1' ...)` |
| Boolean presence flags | `MAX(CASE WHEN cond THEN 1 ELSE 0 END)` |
| Dynamic categories (unknown at query time) | Use `PIVOT` clause — CASE approach requires knowing categories upfront |
