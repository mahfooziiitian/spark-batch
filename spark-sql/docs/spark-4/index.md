# :material-new-box: What's New in Spark 4.0

Apache Spark 4.0 introduces major SQL enhancements — new syntax, data types,
and programming constructs that bring Spark SQL closer to a full procedural language.

!!! tip "Compatibility"

    All features on this page work with open-source Apache Spark 4.0.
    Databricks-specific additions are marked **[Databricks]** where applicable.

______________________________________________________________________

## :material-star: Key Features

| Feature                                                          | Description                                                        |
| ---------------------------------------------------------------- | ------------------------------------------------------------------ |
| [Pipe Syntax `\|>`](pipe/index.md)                               | Chain operations in a readable top-to-bottom pipeline              |
| [String Collation](collation/index.md)                           | ICU-backed case/accent-insensitive string comparisons              |
| [Session Variables](variables/index.md)                          | `DECLARE` / `SET VAR` for session-scoped state                     |
| [EXECUTE IMMEDIATE](execute_immediate/index.md)                  | Dynamic SQL with parameterized queries                             |
| [IDENTIFIER Clause](identifier/index.md)                         | Safe runtime SQL identifier templating                             |
| [VARIANT Data Type](../schema-tables/types/variant/index.md)     | Semi-structured JSON storage with lazy, schema-flexible access     |
| [SQL User-Defined Functions](../functions/sql-udf/index.md)      | Reusable scalar/table functions defined entirely in SQL            |
| [SQL Scripting](../scripting/index.md)                           | `BEGIN … END` blocks with variables, loops, and exception handling |
| [Lateral Column Alias](../schema-tables/column/lateral-alias.md) | Reference an earlier `SELECT` alias later in the same list         |
| [Migration Guide](migration/index.md)                            | Breaking changes from Spark 3.5 → 4.0                              |

______________________________________________________________________

## :material-lightbulb-outline: Highlights

### Pipe Syntax

```sql
FROM sales
|> WHERE region = 'APAC'
|> AGGREGATE SUM(amount) AS total GROUP BY product
|> ORDER BY total DESC
|> LIMIT 10;
```

### Session Variables

```sql
DECLARE total_threshold INT = 1000;

SELECT product, SUM(amount) AS total
FROM sales
GROUP BY product
HAVING total > total_threshold;
```

### EXECUTE IMMEDIATE

```sql
EXECUTE IMMEDIATE
  'SELECT * FROM ' || ? || ' WHERE status = ?'
  USING 'orders', 'active';
```

### VARIANT Data Type

```sql
CREATE TABLE events (id BIGINT, payload VARIANT) USING PARQUET;

SELECT id, payload:user.name::STRING AS user_name
FROM events;
```

### SQL User-Defined Function

```sql
CREATE FUNCTION discounted_price(price DOUBLE, pct DOUBLE)
RETURNS DOUBLE
RETURN price * (1 - pct / 100);

SELECT discounted_price(amount, 10) FROM orders;
```

### SQL Scripting

```sql
BEGIN
  DECLARE total_rows BIGINT DEFAULT 0;
  SET total_rows = (SELECT COUNT(*) FROM orders WHERE status = 'pending');

  IF total_rows > 0 THEN
    INSERT INTO audit_log VALUES ('pending_orders_found', total_rows, current_timestamp());
  END IF;
END;
```

______________________________________________________________________

## :material-arrow-right: Getting Started

Start with the [Pipe Syntax](pipe/index.md) — it's the most impactful change for
day-to-day query writing. Then explore [Session Variables](variables/index.md),
[SQL Scripting](../scripting/index.md), and [EXECUTE IMMEDIATE](execute_immediate/index.md)
for dynamic, procedural SQL patterns.

For upgrading existing code, see the [Migration Guide](migration/index.md).
