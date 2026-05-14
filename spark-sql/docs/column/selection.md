# :material-select-all: Column Selection

Column selection controls which columns appear in the query result — from selecting
all columns with `*` to precisely choosing, excluding, or reordering specific ones.

---

## :material-code-tags: Syntax

```sql
-- All columns
SELECT * FROM orders;

-- Specific columns
SELECT order_id, customer_id, amount FROM orders;

-- All columns EXCEPT some (Spark SQL / Databricks)
SELECT * EXCEPT (internal_id, _ingested_at) FROM orders;

-- All columns from one table plus columns from another
SELECT o.*, c.name AS customer_name
FROM orders AS o
JOIN customers AS c ON o.customer_id = c.customer_id;

-- Qualify with table alias to avoid ambiguity
SELECT o.order_id, c.name FROM orders AS o JOIN customers AS c ON o.customer_id = c.customer_id;
```

---

## :material-information-outline: Behavior

1. `SELECT *` expands to all columns in the table at **query parse time** — schema changes after the query is written may silently include new columns.
2. `SELECT * EXCEPT (col1, col2)` removes the named columns from the `*` expansion — available in Spark SQL 3.x and Databricks.
3. Qualifying columns with a table alias (`o.order_id`) is required when two joined tables share a column name.
4. Column order in the result set matches the order they appear in the `SELECT` list, not the table definition.
5. `SELECT *` in a `CREATE TABLE AS SELECT` (CTAS) copies the source schema including nullability but **not** constraints or partitioning.
6. For wide tables (100+ columns), always list columns explicitly in production pipelines — `SELECT *` couples the pipeline to the table schema.

---

## :material-flask-outline: Practical Examples

### Select specific columns

```sql
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    status
FROM orders
WHERE order_date >= '2024-01-01';
```

### SELECT * EXCEPT — drop internal/audit columns

```sql
-- Expose all business columns but hide ETL internals
SELECT * EXCEPT (_ingested_at, _source_file, _row_hash)
FROM staging_orders;
```

### SELECT * EXCEPT in CTAS

```sql
-- Copy table, dropping sensitive columns
CREATE TABLE orders_public
USING DELTA
AS
SELECT * EXCEPT (credit_card_hash, ip_address)
FROM orders_full;
```

### Table-qualified columns after JOIN

```sql
-- Both orders and returns have an 'amount' column — qualify to disambiguate
SELECT
    o.order_id,
    o.amount        AS order_amount,
    r.amount        AS return_amount,
    o.amount - COALESCE(r.amount, 0) AS net_amount
FROM orders  AS o
LEFT JOIN returns AS r ON o.order_id = r.order_id;
```

### Mix * with extra columns

```sql
-- All order columns plus the customer name from a joined table
SELECT
    o.*,
    c.name      AS customer_name,
    c.segment
FROM orders AS o
JOIN customers AS c ON o.customer_id = c.customer_id;
```

### Reorder columns without renaming

```sql
-- Promote key columns to the front; push audit columns to the end
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    status,
    region,
    created_at,
    updated_at
FROM orders;
```

### Deduplicate column list after SELECT *

```sql
-- After a self-join, column names can clash — select explicitly
SELECT
    a.customer_id,
    a.name          AS name_system_a,
    b.name          AS name_system_b,
    a.email         AS email_system_a,
    b.email         AS email_system_b
FROM system_a_customers AS a
JOIN system_b_customers AS b ON a.customer_id = b.customer_id;
```

### Dynamic column selection with EXCEPT in view

```sql
-- View that always shows all columns except auditing ones,
-- automatically adapting as new columns are added to the source table
CREATE OR REPLACE VIEW orders_view AS
SELECT * EXCEPT (created_by, updated_by, _etl_batch_id)
FROM orders;
```

### CTAS — select and rename in one step

```sql
CREATE TABLE order_summary
USING DELTA
PARTITIONED BY (order_month)
AS
SELECT
    order_id,
    customer_id,
    DATE_TRUNC('month', order_date)     AS order_month,
    SUM(amount)                         AS total_amount,
    COUNT(*)                            AS line_count
FROM orders
GROUP BY order_id, customer_id, DATE_TRUNC('month', order_date);
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Pattern |
|----------|---------|
| Explore data interactively | `SELECT *` |
| Production pipeline | List columns explicitly |
| Drop audit / internal columns | `SELECT * EXCEPT (col1, col2)` |
| Disambiguate joined columns | Qualify with table alias `t.col` |
| Copy table structure + data | `CREATE TABLE ... AS SELECT *` |
| Expose public view | `SELECT * EXCEPT (sensitive_col)` |

!!! warning "Avoid SELECT * in production"
    Schema changes (new columns added to the source) silently propagate through
    `SELECT *` — this can break downstream consumers or insert unexpected columns
    into target tables. Always specify columns explicitly in production pipelines.
