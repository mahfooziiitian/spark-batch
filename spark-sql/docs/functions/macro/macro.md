# :material-code-json: SQL Macros

!!! info "Databricks Runtime only — not part of open-source Apache Spark"

    `CREATE TEMPORARY MACRO` is a **Databricks Runtime** SQL extension. It is
    not part of the Apache Spark grammar — running it against open-source
    Spark (`spark-sql`, `pyspark` without Databricks Runtime) raises
    `[INVALID_STATEMENT_OR_CLAUSE]`. All examples on this page are
    `[Databricks]`.

SQL macros define **reusable SQL expressions** that are expanded inline at query compilation time.
Unlike UDFs which execute row-by-row at runtime, macros are substituted directly into the query
plan and fully optimized by Catalyst.

## :material-sitemap: Overview

```mermaid
graph LR
    A["CREATE TEMPORARY MACRO name(x) expr"] --> B[Reusable Expression]
    B --> C["SELECT name(col) FROM table"]
```

### :material-animation-play: Interactive Visualization — Macro Expansion vs UDF Call

<div id="viz-macro-expansion" class="ts-viz"></div>

Toggle between **Macro** and **UDF** to see the key difference: a macro call
is textually **substituted** into the query plan before optimization runs, so
Catalyst sees the expanded expression and can push it through filters,
constant-fold it, etc. A UDF call remains an opaque, unoptimizable box that
Spark invokes once per row at execution time.

## :material-pin: Syntax

### Create a Macro

```sql
CREATE TEMPORARY MACRO macro_name(param1 TYPE, param2 TYPE, ...)
  expression;
```

### Drop a Macro

```sql
DROP TEMPORARY MACRO [IF EXISTS] macro_name;
```

## :material-magnify: Behavior

1. Macros are **expanded inline** — the expression replaces the macro call in the query plan.
2. **No serialization overhead** — unlike UDFs, there is no data conversion cost.
3. **Session-scoped** — macros are temporary and do not persist across sessions.
4. **Strongly typed** — parameters must have explicit types.
5. **Catalyst-optimized** — the inlined expression benefits from predicate pushdown, constant folding, etc.
6. **No side effects** — macros are pure expressions (no state, no I/O).
7. **Parameters only, no free column references** — the macro body may only reference its declared parameters (plus literals/built-in functions); it cannot reach into the calling query's other columns implicitly. Every value the expression needs must be passed in as an argument at the call site.
8. **Type mismatches fail at analysis time, not silently coerce** — passing an argument that Spark cannot implicitly cast to the declared parameter type raises an analysis error before the query runs (see example below), same as any strongly-typed SQL function call.

## :material-flask-outline: Practical Examples

### :material-toy-brick: 1. Simple Calculation

```sql
CREATE TEMPORARY MACRO double_it(x INT) x * 2;

SELECT double_it(5);
-- Result: 10

SELECT double_it(amount) FROM VALUES (10), (20), (30) AS t(amount);
-- Result: 20, 40, 60
```

### :material-toy-brick: 2. Multi-Parameter — String Formatting

```sql
CREATE TEMPORARY MACRO full_name(first STRING, last STRING)
  CONCAT(first, ' ', last);

SELECT full_name('John', 'Doe');
-- Result: 'John Doe'
```

### :material-toy-brick: 3. Business Logic — Tax Calculation

```sql
CREATE TEMPORARY MACRO tax_amount(price DOUBLE, rate DOUBLE)
  ROUND(price * rate, 2);

SELECT
  product,
  price,
  tax_amount(price, 0.08) AS tax,
  price + tax_amount(price, 0.08) AS total
FROM VALUES ('Widget', 29.99), ('Gadget', 49.99) AS t(product, price);
-- Widget: tax=2.40, total=32.39
-- Gadget: tax=4.00, total=53.99
```

### :material-toy-brick: 4. Conditional Logic — Status Labeling

```sql
CREATE TEMPORARY MACRO status_label(code INT)
  CASE
    WHEN code = 1 THEN 'Active'
    WHEN code = 2 THEN 'Inactive'
    WHEN code = 3 THEN 'Suspended'
    ELSE 'Unknown'
  END;

SELECT id, status_label(status_code) AS status
FROM VALUES (1, 1), (2, 3), (3, 99) AS t(id, status_code);
-- (1, Active), (2, Suspended), (3, Unknown)
```

### :material-toy-brick: 5. Date Helpers

```sql
CREATE TEMPORARY MACRO fiscal_quarter(d DATE)
  CONCAT('Q', CAST(CEIL(MONTH(d) / 3.0) AS INT));

SELECT fiscal_quarter(DATE '2024-07-15');
-- Result: Q3

SELECT dt, fiscal_quarter(dt) AS quarter
FROM VALUES (DATE '2024-01-10'), (DATE '2024-05-20'), (DATE '2024-11-01') AS t(dt);
-- Q1, Q2, Q4
```

### :material-toy-brick: 6. NULL-Safe Defaults

```sql
CREATE TEMPORARY MACRO safe_div(a DOUBLE, b DOUBLE)
  CASE WHEN b = 0 OR b IS NULL THEN NULL ELSE a / b END;

SELECT safe_div(10.0, 3.0);   -- 3.333...
SELECT safe_div(10.0, 0.0);   -- NULL
SELECT safe_div(10.0, NULL);  -- NULL
```

### :material-toy-brick: 7. Composing Macros

```sql
CREATE TEMPORARY MACRO cents_to_dollars(c INT) ROUND(c / 100.0, 2);
CREATE TEMPORARY MACRO with_tax(amount DOUBLE, rate DOUBLE) ROUND(amount * (1 + rate), 2);

SELECT with_tax(cents_to_dollars(4999), 0.08) AS total;
-- 49.99 * 1.08 = 53.99
```

### :material-toy-brick: 8. Range Bucketing — Boolean/BETWEEN Logic

```sql
CREATE TEMPORARY MACRO age_bracket(age INT)
  CASE
    WHEN age BETWEEN 0 AND 17  THEN 'minor'
    WHEN age BETWEEN 18 AND 64 THEN 'adult'
    ELSE 'senior'
  END;

SELECT id, age, age_bracket(age) AS bracket
FROM VALUES (1, 12), (2, 45), (3, 70) AS t(id, age);
-- (1, 12, minor), (2, 45, adult), (3, 70, senior)
```

### :material-alert-outline: 9. Type Mismatch Fails at Analysis Time

```sql
CREATE TEMPORARY MACRO double_it(x INT) x * 2;

SELECT double_it('abc');
-- Error: [DATATYPE_MISMATCH...] cannot resolve 'double_it('abc')' due to
-- data type mismatch: argument 1 requires int type, however 'abc' is of string type
-- => macros are strongly typed just like built-in functions; cast at the call site:
SELECT double_it(CAST('abc' AS INT));  -- still fails ('abc' is not numeric) — wrong data, not wrong type
SELECT double_it(CAST('7' AS INT));    -- 14, works because '7' is castable to INT
```

### :material-broom: Clean Up

```sql
DROP TEMPORARY MACRO IF EXISTS double_it;
DROP TEMPORARY MACRO IF EXISTS full_name;
DROP TEMPORARY MACRO IF EXISTS tax_amount;
DROP TEMPORARY MACRO IF EXISTS status_label;
DROP TEMPORARY MACRO IF EXISTS fiscal_quarter;
DROP TEMPORARY MACRO IF EXISTS safe_div;
DROP TEMPORARY MACRO IF EXISTS cents_to_dollars;
DROP TEMPORARY MACRO IF EXISTS with_tax;
DROP TEMPORARY MACRO IF EXISTS age_bracket;
```

## :material-alert:️ Limitations

1. **Session-scoped only** — cannot create permanent macros; they must be recreated each session.
2. **SQL expressions only** — no procedural logic, loops, or variable assignments.
3. **No overloading** — cannot define two macros with the same name but different signatures.
4. **No recursion** — a macro cannot call itself.
5. **Databricks Runtime only** — `CREATE TEMPORARY MACRO` is not part of open-source Apache Spark at any version; it requires Databricks Runtime (verified: fails to parse on Apache Spark 4.2 OSS with `[INVALID_STATEMENT_OR_CLAUSE]`).
6. **No implicit column access** — a macro can only see the parameters you declare; you cannot write a macro body that references an outer-query column by name unless that column is passed in explicitly as an argument.

## :material-brain: Macros vs UDFs vs Views

| Feature               | SQL Macro                                        | UDF                                          | View                                          |
| --------------------- | ------------------------------------------------ | -------------------------------------------- | --------------------------------------------- |
| Expansion             | Inline at compile time                           | Executed at runtime                          | Subquery at compile time                      |
| Performance           | Fastest (no overhead)                            | Slower (serialization)                       | Good (optimized)                              |
| Parameterized         | :material-check-circle-outline: Typed parameters | :material-check-circle-outline: Any language | :material-close-circle-outline: No parameters |
| Scope                 | Session only                                     | Session or permanent                         | Session or permanent                          |
| Language              | SQL expressions                                  | Python, Scala, Java                          | SQL queries                                   |
| Catalyst optimization | Fully optimized                                  | Opaque to optimizer                          | Fully optimized                               |
| Use case              | Reusable calculations                            | Complex / procedural logic                   | Reusable queries                              |

> **Tip:** Use macros for small, frequently-used calculations (tax, formatting, rounding).
> Use UDFs when you need procedural logic or external libraries. Use views when you need
> reusable query results rather than expressions.
