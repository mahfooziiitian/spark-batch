# :material-function-variant: SQL User-Defined Functions

!!! info "Spark 4.0"

    Native SQL UDFs (scalar and table-valued) are new in Apache Spark 4.0.

Create reusable functions in **pure SQL** — no JVM or Python runtime required.
Spark 4.0 supports both **scalar UDFs** (return a single value) and
**table-valued functions** (return a result set).

______________________________________________________________________

## :material-sitemap: Overview

```mermaid
graph LR
    A["CREATE FUNCTION name(params) RETURNS type RETURN expr"] --> B[Cataloged Routine]
    B --> C["SELECT name(args) FROM table"]
```

### :material-animation-play: Interactive Visualization — SQL UDF vs Python UDF Expansion

<div id="viz-sql-udf-expansion" class="ts-viz"></div>

Toggle between **SQL UDF** and **Python UDF** to see why SQL UDFs are faster:
a SQL UDF body is inlined into the query plan (just like a `CREATE TEMPORARY MACRO`), while a Python UDF stays an opaque per-row call that crosses the
JVM/Python boundary.

______________________________________________________________________

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
SELECT add_tax(100.0, 0.15);  -- 114.99999999999999 (binary floating-point rounding, not a UDF bug)
SELECT ROUND(add_tax(100.0, 0.15), 2);  -- 115.0 -- round DOUBLE results before display
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

______________________________________________________________________

## :material-table-arrow-right: Table-Valued Functions (TVF)

Table-valued functions return a result set and can be used in the `FROM` clause:

### :material-animation-play: Interactive Visualization — One Call, Many Rows

<div id="viz-tvf-rows" class="ts-viz"></div>

A TVF call in the `FROM` clause fans out into multiple output rows — unlike a
scalar UDF, which always returns exactly one value per invocation. Adjust the
date range to see the row count change live.

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

______________________________________________________________________

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

______________________________________________________________________

## :material-magnify: Behavior & Limitations

1. **No recursion** — a function body cannot call itself; `fact` referencing `fact` inside its own `CREATE FUNCTION` fails with `[ROUTINE_NOT_FOUND]` because the routine doesn't exist in the catalog yet while its own body is being resolved.
2. **No overloading by signature** — creating a second `area(x INT, y INT)` when `area(x DOUBLE, y DOUBLE)` already exists fails with `[ROUTINE_ALREADY_EXISTS]`, even though the parameter types differ. Use `CREATE OR REPLACE FUNCTION` or a distinct name instead.
3. **NULL propagation** — like built-in expressions, a `NULL` argument produces a `NULL` result unless the function body explicitly guards with `COALESCE`/`CASE`.
4. **`DOUBLE` arithmetic is standard IEEE-754** — a UDF is not exempt from floating-point rounding; `add_tax(100.0, 0.15)` returns `114.99999999999999`, not a clean `115.0` (see example above). Wrap with `ROUND` when displaying money-like values.
5. **`RETURNS TABLE(...)` requires the query to project exactly those columns** — a TVF's `RETURN` query must match the declared output schema in name/count/order.

### :material-alert-outline: Recursion and Overload Pitfalls

```sql
-- Recursion fails: fact() isn't registered yet while its own body is being defined
CREATE FUNCTION fact(n INT) RETURNS INT RETURN CASE WHEN n <= 1 THEN 1 ELSE n * fact(n - 1) END;
-- Error: [ROUTINE_NOT_FOUND] The routine `default`.`fact` cannot be found.

-- Overloading fails: same name, different signature, no CREATE OR REPLACE
CREATE FUNCTION area(x DOUBLE, y DOUBLE) RETURNS DOUBLE RETURN x * y;
CREATE FUNCTION area(x INT, y INT) RETURNS INT RETURN x * y;
-- Error: [ROUTINE_ALREADY_EXISTS] Cannot create the routine `default`.`area`
-- because a routine of that name already exists.

-- Fix: use CREATE OR REPLACE to redefine, or pick a distinct name
CREATE OR REPLACE FUNCTION area(x INT, y INT) RETURNS INT RETURN x * y;
```

______________________________________________________________________

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

______________________________________________________________________

## :material-compare-horizontal: SQL UDF vs Python/Scala UDF

| Feature        | SQL UDF                    | Python UDF             | Scala UDF       |
| -------------- | -------------------------- | ---------------------- | --------------- |
| Language       | Pure SQL                   | Python                 | Scala/Java      |
| Serialization  | None                       | Arrow/Pickle           | None            |
| Performance    | Best (inlined by Catalyst) | Slower (cross-process) | Good            |
| Table-valued   | Yes                        | No                     | No              |
| Default params | Yes                        | Yes                    | Yes             |
| Persistence    | Catalog                    | Session                | Session         |
| Best for       | SQL logic & reuse          | Complex algorithms     | JVM integration |
