# :material-identifier: IDENTIFIER Clause

!!! info "Spark 4.0"
    The IDENTIFIER clause is new in Apache Spark 4.0.

The **IDENTIFIER clause** converts a string expression into a SQL identifier
(table name, column name, function name) **without SQL injection risk**. It
replaces unsafe string concatenation with safe parameterized identifier resolution.

---

## :material-pin: Syntax

```sql
IDENTIFIER(string_expression)
```

The string expression can be a literal, variable, or parameter marker. It resolves
to a table, column, or function reference at parse time.

---

## :material-code-tags: Table Name as Parameter

```sql
-- With session variables
DECLARE tbl = 'my_table';
CREATE TABLE IDENTIFIER(tbl) (id INT, name STRING);
SELECT * FROM IDENTIFIER(tbl);
DROP TABLE IDENTIFIER(tbl);

-- With EXECUTE IMMEDIATE
EXECUTE IMMEDIATE 'SELECT * FROM IDENTIFIER(:tab)'
    USING 'employees' AS tab;
```

---

## :material-code-tags: Column Name as Parameter

```sql
-- Dynamic column reference
EXECUTE IMMEDIATE 'SELECT IDENTIFIER(:col) FROM employees'
    USING 'salary' AS col;

-- Qualified column reference
EXECUTE IMMEDIATE 'SELECT IDENTIFIER(:col) FROM employees'
    USING 'employees.salary' AS col;
```

---

## :material-code-tags: Function Name as Parameter

```sql
-- Dynamic function call
EXECUTE IMMEDIATE 'SELECT IDENTIFIER(:fn)(-1)'
    USING 'abs' AS fn;
-- Result: 1
```

---

## :material-code-tags: Qualified Names with Concatenation

```sql
-- Build schema.table reference
DECLARE schema_name = 'analytics';
DECLARE table_name = 'events';

SELECT * FROM IDENTIFIER(schema_name || '.' || table_name);

-- Or with EXECUTE IMMEDIATE
EXECUTE IMMEDIATE
    'ALTER TABLE IDENTIFIER(:s || ''.'' || :t) ADD COLUMN c2 INT'
    USING 'default' AS s, 'users' AS t;
```

---

## :material-code-tags: With PySpark / Scala API

```python
# PySpark
spark.sql(
    "CREATE TABLE IDENTIFIER(:tab)(c1 INT)",
    args={"tab": "users"}
)

spark.sql(
    "SELECT IDENTIFIER(:col) FROM IDENTIFIER(:tab)",
    args={"col": "salary", "tab": "employees"}
)
```

```scala
// Scala
spark.sql(
    "SELECT * FROM IDENTIFIER(:tab) WHERE id = :id",
    Map("tab" -> "users", "id" -> 42)
)
```

---

## :material-shield-check: Security Benefits

| Approach | SQL Injection Risk | Recommended |
|----------|:-----------------:|:-----------:|
| `IDENTIFIER(:param)` | :white_check_mark: Safe | Yes |
| String concatenation | :x: Vulnerable | No |
| Hard-coded names | :white_check_mark: Safe | When static |

```sql
-- ✅ Safe: IDENTIFIER validates the string is a legal identifier
SELECT * FROM IDENTIFIER(:user_input);

-- ❌ Unsafe: raw concatenation allows arbitrary SQL
-- SELECT * FROM ' || user_input  -- NEVER DO THIS
```
