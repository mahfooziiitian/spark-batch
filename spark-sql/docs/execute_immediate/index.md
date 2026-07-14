# :material-play-box-outline: EXECUTE IMMEDIATE

!!! info "Spark 4.0"
    `EXECUTE IMMEDIATE` is new in Apache Spark 4.0.

**EXECUTE IMMEDIATE** runs a SQL string dynamically at runtime, with support for
both positional (`?`) and named (`:name`) parameter markers. This is Spark's
equivalent of prepared statements / dynamic SQL.

---

## :material-pin: Syntax

```sql
EXECUTE IMMEDIATE sql_string
    [INTO variable [, ...]]
    [USING expr [AS name] [, ...]];
```

| Clause | Description |
|--------|-------------|
| `sql_string` | A STRING expression containing the SQL to execute |
| `INTO` | Store scalar results into session/scripting variables |
| `USING` | Bind values to `?` (positional) or `:name` (named) markers |

---

## :material-code-tags: Positional Parameters (`?`)

```sql
-- Bind by position
EXECUTE IMMEDIATE 'SELECT SUM(col1) FROM VALUES(?), (?)'
    USING 5, 6;
-- Result: 11
```

---

## :material-code-tags: Named Parameters (`:name`)

```sql
-- Bind by name (order-independent)
DECLARE sqlStr STRING = 'SELECT SUM(col1) FROM VALUES(:first), (:second)';
EXECUTE IMMEDIATE sqlStr USING 5 AS first, 6 AS second;
-- Result: 11
```

---

## :material-code-tags: Storing Results with INTO

```sql
DECLARE total INT;
EXECUTE IMMEDIATE 'SELECT SUM(col1) FROM VALUES(?), (?)'
    INTO total
    USING 5, 6;

SELECT total;  -- 11
```

---

## :material-code-tags: Dynamic Table Names with IDENTIFIER

```sql
-- Describe a table by name
EXECUTE IMMEDIATE 'DESCRIBE IDENTIFIER(:tbl)'
    USING 'my_table' AS tbl;

-- Count rows from a dynamic table
DECLARE tbl_name STRING DEFAULT 'orders';
DECLARE row_count BIGINT;
EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM IDENTIFIER(:t)'
    INTO row_count
    USING tbl_name AS t;
```

---

## :material-code-tags: Building SQL from Variables

```sql
DECLARE arg1 = 5;
DECLARE arg2 = 6;
DECLARE query = 'SELECT SUM(col1) FROM VALUES(?), (?)';

EXECUTE IMMEDIATE query USING arg1, arg2;
```

---

## :material-code-tags: Practical Examples

### Dynamic Report Query

```sql
DECLARE report_table STRING DEFAULT 'monthly_sales';
DECLARE report_month STRING DEFAULT '2024-06';
DECLARE total_revenue DECIMAL(15,2);

EXECUTE IMMEDIATE
    'SELECT SUM(revenue) FROM IDENTIFIER(:tbl) WHERE month = :m'
    INTO total_revenue
    USING report_table AS tbl, report_month AS m;

SELECT total_revenue;
```

### Schema Inspection

```sql
DECLARE schema_name STRING DEFAULT 'analytics';
DECLARE table_name STRING DEFAULT 'events';

EXECUTE IMMEDIATE
    'ALTER TABLE IDENTIFIER(:s || ''.'' || :t) ADD COLUMN tags ARRAY<STRING>'
    USING schema_name AS s, table_name AS t;
```

### In SQL Scripting

```sql
BEGIN
    DECLARE tables ARRAY<STRING> DEFAULT ARRAY('t1', 't2', 't3');
    DECLARE counts ARRAY<BIGINT> DEFAULT ARRAY();
    DECLARE cnt BIGINT;

    FOR i IN (SELECT explode(tables) AS tbl) DO
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM IDENTIFIER(:t)'
            INTO cnt USING i.tbl AS t;
        SET counts = array_append(counts, cnt);
    END FOR;

    SELECT counts;
END;
```

---

## :material-shield-check: Security

`EXECUTE IMMEDIATE` with **IDENTIFIER clause** prevents SQL injection by treating
parameter values as identifiers rather than raw SQL text. Always use `:name`
parameters instead of string concatenation.

```sql
-- ✅ Safe: parameter is treated as an identifier
EXECUTE IMMEDIATE 'SELECT * FROM IDENTIFIER(:tbl)' USING user_input AS tbl;

-- ❌ Unsafe: string concatenation allows injection
-- EXECUTE IMMEDIATE 'SELECT * FROM ' || user_input;  -- DON'T DO THIS
```
