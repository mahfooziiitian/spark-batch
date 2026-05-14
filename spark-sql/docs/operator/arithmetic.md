# :material-plus-minus-variant: Arithmetic Operators

Arithmetic operators perform numeric calculations on columns, literals, and expressions.
Spark SQL supports standard integer and floating-point arithmetic with well-defined
rules for integer division, modulo, and overflow.

---

## :material-code-tags: Syntax

| Operator | Name | Example | Result |
|----------|------|---------|--------|
| `+` | Addition | `10 + 3` | `13` |
| `-` | Subtraction | `10 - 3` | `7` |
| `*` | Multiplication | `10 * 3` | `30` |
| `/` | Division | `10 / 4` | `2.5` |
| `%` | Modulo (remainder) | `10 % 3` | `1` |
| `DIV` | Integer division | `10 DIV 3` | `3` |
| `MOD` | Alias for `%` | `10 MOD 3` | `1` |
| `-x` | Unary negation | `-amount` | negated value |

!!! note "Division type promotion"
    `INT / INT` returns `DOUBLE` in Spark SQL.
    Use `DIV` when you need integer truncation: `10 DIV 3 = 3`.

---

## :material-information-outline: Behavior

1. Division by zero returns `NULL` (not an error) for `DOUBLE`/`FLOAT`; it raises `ArithmeticException` for `INT`/`BIGINT` when `spark.sql.ansi.enabled = true`.
2. Integer overflow wraps silently in non-ANSI mode; in ANSI mode it raises `ArithmeticException`.
3. `NULL` in any arithmetic expression propagates `NULL` to the result — use `COALESCE` to substitute a default.
4. Mixed numeric types are promoted to the wider type: `INT + DOUBLE → DOUBLE`.
5. `DECIMAL` arithmetic preserves precision and scale according to SQL standard rules.

---

## :material-flask-outline: Practical Examples

### Basic column arithmetic

```sql
SELECT
    product_id,
    unit_price,
    tax_rate,
    unit_price * (1 + tax_rate)                     AS price_with_tax,
    unit_price * (1 - discount_pct / 100.0)         AS discounted_price,
    ROUND(unit_price * quantity, 2)                 AS line_total
FROM order_lines;
```

### Integer division and modulo

```sql
-- Page number for a given row (1-indexed, 20 rows per page)
SELECT
    row_id,
    (row_id - 1) DIV 20 + 1    AS page_number,
    (row_id - 1) MOD 20 + 1    AS position_on_page
FROM (SELECT ROW_NUMBER() OVER (ORDER BY created_at) AS row_id FROM items) AS numbered;
```

### Percentage calculation

```sql
SELECT
    department,
    SUM(salary)                                         AS dept_salary,
    SUM(SUM(salary)) OVER ()                            AS total_salary,
    ROUND(SUM(salary) * 100.0 / SUM(SUM(salary)) OVER (), 2) AS pct_of_total
FROM employees
GROUP BY department;
```

### Revenue metrics

```sql
SELECT
    order_id,
    revenue,
    cost,
    revenue - cost                          AS gross_profit,
    ROUND((revenue - cost) / revenue * 100, 2) AS margin_pct,
    ROUND(revenue / NULLIF(units_sold, 0), 4)  AS revenue_per_unit
FROM sales_summary;
```

### NULL propagation and COALESCE

```sql
-- discount may be NULL — treat NULL as 0
SELECT
    order_id,
    subtotal,
    COALESCE(discount, 0)                           AS discount,
    subtotal - COALESCE(discount, 0)                AS final_amount
FROM orders;
```

### Division by zero protection

```sql
-- Use NULLIF to avoid divide-by-zero
SELECT
    product_id,
    returned_qty,
    sold_qty,
    ROUND(returned_qty * 100.0 / NULLIF(sold_qty, 0), 2) AS return_rate_pct
FROM product_returns;
```

### Compound growth rate

```sql
SELECT
    product_id,
    revenue_2022,
    revenue_2024,
    ROUND(
        (POWER(revenue_2024 / NULLIF(revenue_2022, 0), 1.0 / 2) - 1) * 100,
        2
    ) AS cagr_pct
FROM annual_revenue;
```

### Bucketing rows by arithmetic

```sql
-- Assign rows to 10 equal-width buckets based on amount
SELECT
    order_id,
    amount,
    FLOOR((amount - min_amt) / NULLIF(max_amt - min_amt, 0) * 10) AS bucket
FROM orders
CROSS JOIN (SELECT MIN(amount) AS min_amt, MAX(amount) AS max_amt FROM orders);
```

### Running total with arithmetic on window result

```sql
SELECT
    order_date,
    daily_revenue,
    SUM(daily_revenue) OVER (ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue,
    daily_revenue - LAG(daily_revenue, 1, 0) OVER (ORDER BY order_date) AS day_over_day_change
FROM daily_sales;
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Pattern |
|----------|---------|
| Price with tax / discount | `price * (1 + tax_rate)` |
| Safe percentage | `numerator * 100.0 / NULLIF(denominator, 0)` |
| Integer page / bucket index | `(n - 1) DIV page_size + 1` |
| Profit margin | `(revenue - cost) / NULLIF(revenue, 0)` |
| NULL-safe arithmetic | `COALESCE(col, 0)` before arithmetic |

!!! warning "Avoid integer division by accident"
    `10 / 4` in Spark SQL returns `2.5` (DOUBLE), but if you cast both sides to `INT`
    first, `CAST(10 AS INT) / CAST(4 AS INT)` still returns `2.5`.
    Use `10 DIV 4` to get integer `2` intentionally.
