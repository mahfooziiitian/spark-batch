# :material-variable: Session Variables

!!! info "Spark 4.0"
    Session-level `DECLARE` / `SET VAR` is new in Apache Spark 4.0.

Session variables persist for the duration of the Spark session and can be
referenced in any SQL statement. They are distinct from SQL scripting variables
(which are scoped to `BEGIN...END` blocks).

---

## :material-pin: DECLARE

```sql
-- Declare with literal value
DECLARE five = 5;

-- Declare with explicit type
DECLARE some_var STRING;

-- Declare with DEFAULT
DECLARE VARIABLE size DEFAULT 6;

-- Replace existing variable
DECLARE OR REPLACE five = 55;
```

### Supported Types

Any Spark SQL data type: `INT`, `BIGINT`, `DOUBLE`, `STRING`, `BOOLEAN`,
`DATE`, `TIMESTAMP`, `ARRAY<T>`, `MAP<K,V>`, `STRUCT<...>`, `VARIANT`, etc.

---

## :material-pencil: SET VAR

```sql
-- Simple assignment
SET VAR five = 10;
SET VARIABLE five = 20;

-- From a query result
SET VAR five = (SELECT max(id) FROM range(100));

-- Reset to DEFAULT
SET VAR five = DEFAULT;
```

### Multi-Variable Assignment

```sql
DECLARE var1 INT DEFAULT 7;
DECLARE var2 STRING;

SET VAR (var1, var2) = (
    SELECT max(c1), CAST(min(c1) AS STRING)
    FROM VALUES(1), (2) AS t(c1)
);
```

---

## :material-magnify: Using Variables in Queries

```sql
DECLARE threshold INT DEFAULT 1000;

-- Reference directly in SQL
SELECT * FROM sales WHERE amount > threshold;

-- Qualified reference
DECLARE system.session.my_var INT DEFAULT 0;
SELECT session.my_var;
```

---

## :material-delete: Dropping Variables

```sql
DROP TEMPORARY VARIABLE five;
DROP TEMPORARY VARIABLE IF EXISTS my_var;
```

---

## :material-code-tags: Practical Examples

### Dynamic Filtering

```sql
DECLARE min_date DATE DEFAULT DATE'2024-01-01';
DECLARE max_date DATE DEFAULT current_date();

SELECT *
FROM orders
WHERE order_date BETWEEN min_date AND max_date;
```

### Configuration-Driven Queries

```sql
DECLARE target_region STRING DEFAULT 'EMEA';
DECLARE top_n INT DEFAULT 10;

SELECT customer_name, total_sales
FROM regional_sales
WHERE region = target_region
ORDER BY total_sales DESC
LIMIT top_n;
```

### Combined with EXECUTE IMMEDIATE

```sql
DECLARE tbl STRING DEFAULT 'sales_2024';
DECLARE result BIGINT;

EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM IDENTIFIER(:t)' INTO result USING tbl AS t;
SELECT result;  -- row count of sales_2024
```

---

## :material-compare-horizontal: Session Variables vs Scripting Variables

| Feature | Session Variables | Scripting Variables |
|---------|-------------------|---------------------|
| Scope | Entire session | `BEGIN...END` block |
| Syntax | `DECLARE x = ...` | `DECLARE x INT DEFAULT ...` inside `BEGIN` |
| Lifetime | Until session ends or `DROP` | Until block ends |
| Visibility | All queries in session | Only within the block |
| Reassignment | `SET VAR` | `SET` |
