# Common Table Expressions (CTEs)

CTEs define named temporary result sets that can be referenced multiple times within a single query statement.

---

## 📌 Syntax

**Single CTE:**

```sql
WITH cte_name AS (
    SELECT ...
    FROM   ...
    WHERE  ...
)
SELECT * FROM cte_name;
```

**Multiple chained CTEs:**

```sql
WITH cte1 AS (
    SELECT ...
),
cte2 AS (
    SELECT ...
    FROM   cte1    -- later CTEs can reference earlier ones
    WHERE  ...
)
SELECT * FROM cte2;
```

**CTE in DML (INSERT / MERGE):**

```sql
WITH prepared AS (
    SELECT ...
)
INSERT INTO target_table
SELECT * FROM prepared;
```

---

## 🔍 Behavior

1. **No materialization by default** — Spark may inline a CTE at every reference site, re-evaluating it each time. Use a temp view or `CACHE TABLE` when the CTE is expensive and referenced multiple times.
2. **Forward references are not allowed** — a CTE can only reference CTEs defined earlier in the same `WITH` block.
3. **Statement scope** — CTEs are visible only within the single statement they appear in. They are not accessible to other queries in the session.
4. **Recursive CTEs** — Spark SQL 3.5+ supports `WITH RECURSIVE` for iterative result-set generation such as traversing hierarchies or producing sequences.

---

## 🧪 Practical Examples

```sql
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (1, 'Alice',   '2024-01-15', 250.00),
  (2, 'Bob',     '2024-01-16', 120.00),
  (3, 'Alice',   '2024-01-17', 300.00),
  (4, 'Charlie', '2024-01-18',  80.00),
  (5, 'Bob',     '2024-01-19', 450.00),
  (6, 'Alice',   '2024-01-20', 175.00)
AS orders(order_id, customer, order_date, amount);
```

### Example 1 — Single CTE: Filter Then Transform

```sql
WITH recent_orders AS (
    SELECT order_id, customer, amount
    FROM   orders
    WHERE  order_date >= '2024-01-17'
)
SELECT
    customer,
    SUM(amount) AS total_amount,
    COUNT(*)    AS order_count
FROM recent_orders
GROUP BY customer;
-- Result:
-- | customer | total_amount | order_count |
-- |----------|--------------|-------------|
-- | Alice    |       475.00 |           2 |
-- | Charlie  |        80.00 |           1 |
-- | Bob      |       450.00 |           1 |
```

### Example 2 — Multiple Chained CTEs: Step-by-Step Pipeline

```sql
WITH order_totals AS (
    SELECT
        customer,
        SUM(amount) AS total_spent
    FROM   orders
    GROUP BY customer
),
ranked_customers AS (
    SELECT
        customer,
        total_spent,
        RANK() OVER (ORDER BY total_spent DESC) AS spend_rank
    FROM order_totals
)
SELECT customer, total_spent, spend_rank
FROM   ranked_customers
WHERE  spend_rank <= 2;
-- Result:
-- | customer | total_spent | spend_rank |
-- |----------|-------------|------------|
-- | Alice    |      725.00 |          1 |
-- | Bob      |      570.00 |          2 |
```

### Example 3 — CTE Referenced Multiple Times (Self-Join)

Compare each customer's total spend against the overall average by referencing the same CTE twice:

```sql
WITH customer_totals AS (
    SELECT
        customer,
        SUM(amount) AS total_spent
    FROM   orders
    GROUP BY customer
)
SELECT
    ct.customer,
    ct.total_spent,
    ROUND(avg_all.avg_spend, 2)             AS overall_avg,
    ROUND(ct.total_spent - avg_all.avg_spend, 2) AS diff_from_avg
FROM customer_totals AS ct
CROSS JOIN (SELECT AVG(total_spent) AS avg_spend FROM customer_totals) AS avg_all;
-- Result:
-- | customer | total_spent | overall_avg | diff_from_avg |
-- |----------|-------------|-------------|---------------|
-- | Alice    |      725.00 |      458.33 |        266.67 |
-- | Bob      |      570.00 |      458.33 |        111.67 |
-- | Charlie  |       80.00 |      458.33 |       -378.33 |
```

### Example 4 — CTE in a MERGE Statement

Prepare source rows with a CTE before merging into a Delta target:

```sql
WITH new_orders AS (
    SELECT order_id, customer, order_date, amount
    FROM   orders
    WHERE  order_date = '2024-01-20'
)
MERGE INTO orders_target AS t
USING new_orders AS s
    ON t.order_id = s.order_id
WHEN MATCHED THEN
    UPDATE SET t.amount = s.amount
WHEN NOT MATCHED THEN
    INSERT (order_id, customer, order_date, amount)
    VALUES (s.order_id, s.customer, s.order_date, s.amount);
```

### Example 5 — Recursive CTE: Number Sequence (Spark 3.5+)

Generate integers from 1 to 5 using `WITH RECURSIVE`:

```sql
WITH RECURSIVE numbers AS (
    SELECT 1 AS n                -- anchor: starting value
    UNION ALL
    SELECT n + 1
    FROM   numbers
    WHERE  n < 5                 -- termination condition
)
SELECT n FROM numbers;
-- Result:
-- | n |
-- |---|
-- | 1 |
-- | 2 |
-- | 3 |
-- | 4 |
-- | 5 |
```

---

## CTE vs Subquery

| Aspect | CTE | Subquery |
|--------|-----|----------|
| Readability | Named, defined once at the top | Anonymous, embedded inline |
| Reusability | Can be referenced multiple times in one statement | Must be duplicated |
| Recursion | Supported (`WITH RECURSIVE`, Spark 3.5+) | Not supported |
| Materialization | Not guaranteed — Spark may inline | Inlined by the optimizer |
| Debugging | Easy to isolate and test each step | Hard to inspect intermediate results |

---

## 🧠 When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Breaking a complex query into readable steps | Multiple chained CTEs |
| Reusing an intermediate result set | Single CTE referenced twice in one statement |
| Generating sequences or traversing hierarchies | `WITH RECURSIVE` (Spark 3.5+) |
| Cleaning source data before a MERGE | CTE in the `USING` clause |
| Performance-sensitive repeated logic | Materialise as a temp view or `CACHE TABLE` |
