# Macros & Dynamic SQL

Macros and dynamic SQL helpers let you define **reusable SQL logic** and safely construct
**dynamic identifiers** — reducing duplication and avoiding injection risks.

## 📌 What's Available

| Feature | Description | Scope |
|---------|-------------|-------|
| `CREATE TEMPORARY MACRO` | Reusable SQL expression expanded inline at compile time | Session |
| `identifier()` | Safe dynamic table/column name resolution | Databricks |

## 🔍 SQL Macros Overview

SQL macros define parameterized expressions that the query planner **inlines** before execution.
Unlike UDFs, macros have zero serialization overhead and are fully optimized by Catalyst.

```sql
CREATE TEMPORARY MACRO double_it(x INT) x * 2;
SELECT double_it(5);  -- Result: 10
```

See the [SQL Macros](macro.md) page for full syntax, examples, and comparison with UDFs.

---

## 🔍 The `identifier()` Function (Databricks)

In Databricks, `identifier()` is a helper for safely injecting **table or column names** into
dynamically constructed SQL — ensuring correct quoting and preventing SQL injection.

### 📌 Syntax

```sql
identifier(string_expression)
```

### Why Use It?

1. **Avoids quoting errors** for names that are reserved keywords (`order`, `select`, etc.).
2. **Prevents SQL injection** when table/column names come from user input or parameters.
3. **Maintains compatibility** across Databricks runtimes.

### 🧪 Examples

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

## 🧠 When to Use

| Scenario | Tool |
|----------|------|
| Reusable calculation / business rule | SQL Macro |
| Dynamic table or column names | `identifier()` |
| Cross-session persistent logic | UDF (macros are session-scoped) |
| Performance-critical expressions | SQL Macro (inline, Catalyst-optimized) |
