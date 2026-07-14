# :material-function-variant: SQL User-Defined Functions

!!! info "Spark 4.0"
    Native SQL UDFs (scalar and table-valued) are new in Apache Spark 4.0.

Create reusable functions in **pure SQL** — no JVM or Python runtime required.
Spark 4.0 supports both **scalar UDFs** (return a single value) and
**table-valued functions** (return a result set).

---

## :material-pin: Scalar UDF

```sql
CREATE FUNCTION area(x DOUBLE, y DOUBLE)
    RETURNS DOUBLE
    RETURN x * y;

SELECT area(width, height) FROM shapes;
```

### With Default Parameters

```sql
CREATE FUNCTION add_tax(price DOUBLE, rate DOUBLE DEFAULT 0.08)
    RETURNS DOUBLE
    RETURN price * (1 + rate);

SELECT add_tax(100.0);        -- 108.0 (default rate)
SELECT add_tax(100.0, 0.15);  -- 115.0
```

### Non-Deterministic Functions

```sql
CREATE FUNCTION roll_dice()
    RETURNS INT
    NOT DETERMINISTIC
    COMMENT 'Roll a 6-sided die'
    RETURN (rand() * 6)::INT + 1;

SELECT roll_dice();
```

---

## :material-table-arrow-right: Table-Valued Functions (TVF)

Table-valued functions return a result set and can be used in the `FROM` clause:

```sql
CREATE FUNCTION weekdays(start DATE, end DATE)
    RETURNS TABLE(day_of_week STRING, day DATE)
    RETURN
        SELECT extract(DAYOFWEEK_ISO FROM day), day
        FROM (SELECT sequence(weekdays.start, weekdays.end)) AS T(days)
             LATERAL VIEW explode(days) AS day
        WHERE extract(DAYOFWEEK_ISO FROM day) BETWEEN 1 AND 5;

-- Use in a query
SELECT * FROM weekdays(DATE'2024-01-01', DATE'2024-01-14');
```

---

## :material-cog: Function Management

### Temporary Functions (Session-Scoped)

```sql
CREATE TEMPORARY FUNCTION hello()
    RETURNS STRING
    RETURN 'Hello World!';

SELECT hello();  -- 'Hello World!'
-- Dropped automatically when session ends
```

### Replace Existing Function

```sql
CREATE OR REPLACE FUNCTION area(x DOUBLE, y DOUBLE)
    RETURNS DOUBLE
    RETURN x * y;
```

### Drop a Function

```sql
DROP FUNCTION IF EXISTS area;
DROP TEMPORARY FUNCTION hello;
```

### Describe a Function

```sql
DESCRIBE FUNCTION area;
DESCRIBE FUNCTION EXTENDED area;
```

---

## :material-code-tags: Practical Examples

### Revenue Calculator

```sql
CREATE FUNCTION calc_revenue(
    qty INT,
    price DECIMAL(10,2),
    discount DOUBLE DEFAULT 0.0
)
    RETURNS DECIMAL(10,2)
    RETURN qty * price * (1 - discount);

SELECT
    product_name,
    calc_revenue(quantity, unit_price, 0.1) AS discounted_revenue
FROM orders;
```

### Date Range Generator (TVF)

```sql
CREATE FUNCTION date_range(start_dt DATE, end_dt DATE)
    RETURNS TABLE(dt DATE)
    RETURN
        SELECT explode(sequence(start_dt, end_dt, INTERVAL 1 DAY));

-- Generate all dates in January 2024
SELECT * FROM date_range(DATE'2024-01-01', DATE'2024-01-31');
```

---

## :material-compare-horizontal: SQL UDF vs Python/Scala UDF

| Feature | SQL UDF | Python UDF | Scala UDF |
|---------|---------|------------|-----------|
| Language | Pure SQL | Python | Scala/Java |
| Serialization | None | Arrow/Pickle | None |
| Performance | Best (inlined by Catalyst) | Slower (cross-process) | Good |
| Table-valued | Yes | No | No |
| Default params | Yes | Yes | Yes |
| Persistence | Catalog | Session | Session |
| Best for | SQL logic & reuse | Complex algorithms | JVM integration |
