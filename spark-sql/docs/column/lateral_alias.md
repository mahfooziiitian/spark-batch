# :material-arrow-right-bold: Lateral Column Alias

!!! info "Spark 4.0"
    Lateral column alias support is new in Apache Spark 4.0.

A **lateral column alias** lets you reference an alias defined earlier in the
**same SELECT list** — eliminating the need for subqueries or CTEs just to reuse
a computed value.

---

## :material-pin: Syntax

```sql
SELECT
    expression AS alias,
    alias + 1 AS derived    -- references 'alias' from the line above
FROM table;
```

---

## :material-code-tags: Before vs After

### Spark 3.x — Subquery Required

```sql
-- Had to wrap in a subquery to reference 'subtotal'
SELECT subtotal, subtotal * 0.08 AS tax
FROM (
    SELECT quantity * unit_price AS subtotal
    FROM orders
);
```

### Spark 4.0 — Lateral Column Alias

```sql
-- Direct reference to 'subtotal' in the same SELECT
SELECT
    quantity * unit_price AS subtotal,
    subtotal * 0.08 AS tax,
    subtotal + (subtotal * 0.08) AS total
FROM orders;
```

---

## :material-code-tags: Chaining Multiple Aliases

```sql
SELECT
    base_price AS price,
    price * quantity AS subtotal,
    subtotal * tax_rate AS tax,
    subtotal + tax AS total
FROM line_items;
```

---

## :material-code-tags: With Pipe Syntax

Lateral column aliases work naturally with pipe `|> SELECT` and `|> EXTEND`:

```sql
-- In pipe SELECT
TABLE transactions
|> SELECT
     amount * quantity AS subtotal,
     subtotal * 0.08 AS tax,
     subtotal + tax AS total;

-- In pipe EXTEND
TABLE products
|> EXTEND price * 1.1 AS adjusted_price
|> EXTEND adjusted_price - price AS increase;
```

---

## :material-code-tags: Practical Examples

### Revenue Metrics

```sql
SELECT
    region,
    SUM(amount) AS revenue,
    COUNT(*) AS order_count,
    revenue / order_count AS avg_order_value,
    revenue * 0.3 AS estimated_profit
FROM sales
GROUP BY region;
```

### Date Calculations

```sql
SELECT
    event_date,
    datediff(current_date(), event_date) AS days_ago,
    CASE
        WHEN days_ago <= 7  THEN 'This Week'
        WHEN days_ago <= 30 THEN 'This Month'
        ELSE 'Older'
    END AS recency_bucket
FROM events;
```

---

## :material-alert-outline: Limitations

- Lateral aliases work in `SELECT` and pipe `|> SELECT` / `|> EXTEND`
- They do **not** work in `WHERE`, `HAVING`, or `JOIN ON` clauses — use a CTE or subquery for those
- An alias can only reference aliases defined **before** it (left-to-right)
