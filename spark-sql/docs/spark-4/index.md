# :material-new-box: What's New in Spark 4.0

Apache Spark 4.0 introduces major SQL enhancements — new syntax, data types,
and programming constructs that bring Spark SQL closer to a full procedural language.

!!! tip "Compatibility"
    All features on this page work with open-source Apache Spark 4.0.
    Databricks-specific additions are marked **[Databricks]** where applicable.

---

## :material-star: Key Features

| Feature | Description |
|---------|-------------|
| [Pipe Syntax `\|>`](pipe/index.md) | Chain operations in a readable top-to-bottom pipeline |
| [String Collation](collation/index.md) | ICU-backed case/accent-insensitive string comparisons |
| [Session Variables](variables/index.md) | `DECLARE` / `SET VAR` for session-scoped state |
| [EXECUTE IMMEDIATE](execute_immediate/index.md) | Dynamic SQL with parameterized queries |
| [IDENTIFIER Clause](identifier/index.md) | Safe runtime SQL identifier templating |
| [Migration Guide](migration/index.md) | Breaking changes from Spark 3.5 → 4.0 |

---

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

---

## :material-arrow-right: Getting Started

Start with the [Pipe Syntax](pipe/index.md) — it's the most impactful change for
day-to-day query writing. Then explore [Session Variables](variables/index.md) and
[EXECUTE IMMEDIATE](execute_immediate/index.md) for dynamic SQL patterns.

For upgrading existing code, see the [Migration Guide](migration/index.md).
