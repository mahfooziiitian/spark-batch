# :material-function-variant: Derived Columns

A derived column is any `SELECT` expression that computes a value rather than reading
it directly from a table column — arithmetic, functions, conditional logic, window
expressions, and subqueries all produce derived columns.

---

## :material-information-outline: Behavior

1. Derived columns are evaluated **after** `FROM`, `WHERE`, and `JOIN` — they see the full filtered row.
2. Derived columns are **not** visible in `WHERE` or `HAVING` in the same `SELECT` clause — use a CTE or subquery to filter on them.
3. Spark's Catalyst optimizer applies **column pruning** — if a derived column is not used downstream (e.g., in a subquery), it is eliminated from the plan.
4. Expressions involving `NULL` propagate `NULL` — use `COALESCE` or `IFNULL` to substitute defaults.
5. Window function–based derived columns require an `OVER (...)` clause and are computed after `GROUP BY`.

---

## :material-flask-outline: Practical Examples

### Arithmetic derived columns

```sql
SELECT
    order_id,
    unit_price,
    quantity,
    discount_pct,
    unit_price * quantity                               AS subtotal,
    unit_price * quantity * (1 - discount_pct / 100.0) AS net_amount,
    unit_price * quantity * tax_rate                    AS tax_amount,
    unit_price * quantity * (1 - discount_pct / 100.0)
        + unit_price * quantity * tax_rate              AS total_amount
FROM order_lines;
```

### String derived columns

```sql
SELECT
    customer_id,
    TRIM(UPPER(first_name)) || ' ' || TRIM(UPPER(last_name))   AS full_name,
    LOWER(email)                                                AS email_normalised,
    SUBSTRING(phone, 1, 3) || '-***-' || SUBSTRING(phone, 7)   AS masked_phone
FROM customers;
```

### Date derived columns

```sql
SELECT
    order_date,
    YEAR(order_date)                    AS order_year,
    MONTH(order_date)                   AS order_month,
    DAYOFWEEK(order_date)               AS day_of_week,
    DATE_TRUNC('month', order_date)     AS month_start,
    LAST_DAY(order_date)                AS month_end,
    DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order
FROM orders;
```

### Conditional derived columns (CASE WHEN)

```sql
SELECT
    customer_id,
    lifetime_value,
    order_count,
    CASE
        WHEN lifetime_value >= 10000 AND order_count >= 10 THEN 'Platinum'
        WHEN lifetime_value >= 5000  OR  order_count >= 20 THEN 'Gold'
        WHEN lifetime_value >= 1000                        THEN 'Silver'
        ELSE                                                    'Bronze'
    END                                         AS tier,
    CASE WHEN is_subscribed THEN 'Yes' ELSE 'No' END AS subscribed_label
FROM customer_summary;
```

### IF and IFF shorthand

```sql
SELECT
    product_id,
    stock_qty,
    IF(stock_qty = 0, 'Out of Stock', 'In Stock') AS availability,
    IFF(is_featured, price * 0.9, price)          AS effective_price
FROM products;
```

### NULL-safe derived columns

```sql
SELECT
    order_id,
    COALESCE(discount_amt, 0)                       AS discount,
    NULLIF(promo_code, '')                          AS promo_code_clean,
    COALESCE(ship_name, bill_name, customer_name)   AS recipient
FROM orders;
```

### Aggregate derived columns

```sql
SELECT
    department,
    COUNT(*)                            AS headcount,
    AVG(salary)                         AS avg_salary,
    MAX(salary) - MIN(salary)           AS salary_range,
    SUM(salary) / SUM(SUM(salary)) OVER () * 100 AS pct_of_total_payroll
FROM employees
GROUP BY department;
```

### Window function derived columns

```sql
SELECT
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                   AS cumulative_spend,
    amount - LAG(amount, 1, 0) OVER (
        PARTITION BY customer_id ORDER BY order_date
    )                                                   AS change_vs_prev_order,
    RANK() OVER (
        PARTITION BY customer_id ORDER BY amount DESC
    )                                                   AS spend_rank
FROM orders;
```

### Derived column from a scalar subquery

```sql
SELECT
    c.customer_id,
    c.name,
    (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.customer_id) AS order_count,
    (SELECT MAX(amount) FROM orders o WHERE o.customer_id = c.customer_id) AS max_order
FROM customers AS c;
```

### Multi-step derivation using CTE

```sql
WITH base AS (
    SELECT
        order_id,
        unit_price * quantity                               AS subtotal,
        unit_price * quantity * discount_pct / 100.0       AS discount_amt
    FROM order_lines
),
enriched AS (
    SELECT
        order_id,
        subtotal,
        discount_amt,
        subtotal - discount_amt                             AS net_amount,
        (subtotal - discount_amt) * 0.2                    AS vat_amount
    FROM base
)
SELECT
    order_id,
    subtotal,
    discount_amt,
    net_amount,
    vat_amount,
    net_amount + vat_amount                                 AS total_due
FROM enriched;
```

### Derived column using REGEXP_EXTRACT

```sql
SELECT
    log_line,
    REGEXP_EXTRACT(log_line, '(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})', 1) AS ip_address,
    REGEXP_EXTRACT(log_line, 'status=(\\d{3})', 1)                               AS status_code,
    CAST(REGEXP_EXTRACT(log_line, 'duration=(\\d+)', 1) AS INT)                  AS duration_ms
FROM access_logs;
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Pattern |
|----------|---------|
| Compute totals / ratios | Arithmetic expression |
| Normalise strings | `TRIM`, `UPPER`, `LOWER`, `||` |
| Derive date parts | `YEAR`, `MONTH`, `DATE_TRUNC` |
| Classify rows into segments | `CASE WHEN` |
| Replace NULL with a default | `COALESCE(col, default)` |
| Running totals / ranks | Window function derived column |
| Multi-step computed column | CTE per derivation step |
| Extract from unstructured text | `REGEXP_EXTRACT` |
