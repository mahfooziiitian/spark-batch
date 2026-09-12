# :material-code-json: Macros & Dynamic SQL

Macros and dynamic SQL helpers let you define **reusable SQL logic** and safely construct
**dynamic identifiers** — reducing duplication and avoiding injection risks.

## :material-sitemap: Overview

```mermaid
graph LR
    A["CREATE TEMPORARY MACRO name(x) expr"] --> B[Reusable Expression]
    B --> C["SELECT name(col) FROM table"]
```

## :material-pin: What's Available

| Feature                  | Description                                             | Scope                            |
| ------------------------ | ------------------------------------------------------- | -------------------------------- |
| `CREATE TEMPORARY MACRO` | Reusable SQL expression expanded inline at compile time | Session, Databricks Runtime only |
| `identifier()`           | Safe dynamic table/column name resolution               | Databricks                       |

## :material-magnify: SQL Macros Overview

SQL macros define parameterized expressions that the query planner **inlines** before execution.
Unlike UDFs, macros have zero serialization overhead and are fully optimized by Catalyst.

```sql
CREATE TEMPORARY MACRO double_it(x INT) x * 2;
SELECT double_it(5);  -- Result: 10
```

See the [SQL Macros](macro.md) page for full syntax, examples, and comparison with UDFs.

______________________________________________________________________

## :material-magnify: The `identifier()` Function (Databricks)

In Databricks, `identifier()` is a helper for safely injecting **table or column names** into
dynamically constructed SQL — ensuring correct quoting and preventing SQL injection.

### :material-animation-play: Interactive Visualization — Safe vs Unsafe Dynamic SQL

<div id="viz-identifier" class="ts-viz"></div>

Compare naive string concatenation (vulnerable to injection and reserved-keyword
syntax errors) against `identifier()` (parsed as a name, always safely quoted).
Try the malicious input to see the difference.

### :material-pin: Syntax

```sql
identifier(string_expression)
```

### Why Use It?

1. **Avoids quoting errors** for names that are reserved keywords (`order`, `select`, etc.).
2. **Prevents SQL injection** when table/column names come from user input or parameters.
3. **Maintains compatibility** across Databricks runtimes.

### :material-flask-outline: Examples

#### Python Notebook — Dynamic Table Name

```python
table_name = "sales_data"
spark.sql(f"SELECT COUNT(*) AS total FROM {identifier(table_name)}").show()
```

#### SQL — Parameterized Column Access

```sql
-- With a widget or parameter
SELECT identifier('my_column') FROM my_table;
```

#### SQL — Use with Reserved Keywords

```sql
-- Without identifier(): SELECT order FROM orders  → syntax error
-- With identifier():
SELECT identifier('order') FROM orders;
```

#### SQL — Dynamic Table Name in DDL

```sql
-- identifier() also works in DDL, not just SELECT
DECLARE OR REPLACE VARIABLE tbl STRING = 'staging_events';
CREATE TABLE IF NOT EXISTS identifier(tbl) (id BIGINT, payload STRING);
DROP TABLE IF EXISTS identifier(tbl);
```

#### SQL — Fully Qualified Three-Part Name

```sql
-- identifier() also resolves catalog.schema.table strings from a single expression
SELECT COUNT(*) FROM identifier('main.sales.orders');
```

### :material-alert-circle: Behavior Notes

1. **Must be constant-foldable** — the string expression must be resolvable at analysis
    time (a literal, session variable, or widget value); it cannot reference a column
    value that varies per row.
2. **Parses the whole string as one name** — passing `'main.sales.orders'` resolves a
    three-part name; it does not concatenate three separately-quoted identifiers.
3. **Not a substitute for `CONCAT`** — `identifier()` produces a name reference, not a
    string value; you cannot do `SELECT identifier('col') || '_suffix'` to build a new
    name — build the target string first, then wrap the whole thing in one `identifier()`.

## :material-brain: When to Use

| Scenario                             | Tool                                   |
| ------------------------------------ | -------------------------------------- |
| Reusable calculation / business rule | SQL Macro                              |
| Dynamic table or column names        | `identifier()`                         |
| Cross-session persistent logic       | UDF (macros are session-scoped)        |
| Performance-critical expressions     | SQL Macro (inline, Catalyst-optimized) |
