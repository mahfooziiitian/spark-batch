# SQL Macros

SQL macros define **reusable SQL expressions** that are expanded inline at query compilation time.
Unlike UDFs which execute row-by-row at runtime, macros are substituted directly into the query
plan and fully optimized by Catalyst.

## 📌 Syntax

### Create a Macro

```sql
CREATE TEMPORARY MACRO macro_name(param1 TYPE, param2 TYPE, ...)
  expression;
```

### Drop a Macro

```sql
DROP TEMPORARY MACRO [IF EXISTS] macro_name;
```

## 🔍 Behavior

1. Macros are **expanded inline** — the expression replaces the macro call in the query plan.
2. **No serialization overhead** — unlike UDFs, there is no data conversion cost.
3. **Session-scoped** — macros are temporary and do not persist across sessions.
4. **Strongly typed** — parameters must have explicit types.
5. **Catalyst-optimized** — the inlined expression benefits from predicate pushdown, constant folding, etc.
6. **No side effects** — macros are pure expressions (no state, no I/O).

## 🧪 Practical Examples

### 🧱 1. Simple Calculation

```sql
CREATE TEMPORARY MACRO double_it(x INT) x * 2;

SELECT double_it(5);
-- Result: 10

SELECT double_it(amount) FROM VALUES (10), (20), (30) AS t(amount);
-- Result: 20, 40, 60
```

### 🧱 2. Multi-Parameter — String Formatting

```sql
CREATE TEMPORARY MACRO full_name(first STRING, last STRING)
  CONCAT(first, ' ', last);

SELECT full_name('John', 'Doe');
-- Result: 'John Doe'
```

### 🧱 3. Business Logic — Tax Calculation

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

### 🧱 4. Conditional Logic — Status Labeling

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

### 🧱 5. Date Helpers

```sql
CREATE TEMPORARY MACRO fiscal_quarter(d DATE)
  CONCAT('Q', CAST(CEIL(MONTH(d) / 3.0) AS INT));

SELECT fiscal_quarter(DATE '2024-07-15');
-- Result: Q3

SELECT dt, fiscal_quarter(dt) AS quarter
FROM VALUES (DATE '2024-01-10'), (DATE '2024-05-20'), (DATE '2024-11-01') AS t(dt);
-- Q1, Q2, Q4
```

### 🧱 6. NULL-Safe Defaults

```sql
CREATE TEMPORARY MACRO safe_div(a DOUBLE, b DOUBLE)
  CASE WHEN b = 0 OR b IS NULL THEN NULL ELSE a / b END;

SELECT safe_div(10.0, 3.0);   -- 3.333...
SELECT safe_div(10.0, 0.0);   -- NULL
SELECT safe_div(10.0, NULL);  -- NULL
```

### 🧱 7. Composing Macros

```sql
CREATE TEMPORARY MACRO cents_to_dollars(c INT) ROUND(c / 100.0, 2);
CREATE TEMPORARY MACRO with_tax(amount DOUBLE, rate DOUBLE) ROUND(amount * (1 + rate), 2);

SELECT with_tax(cents_to_dollars(4999), 0.08) AS total;
-- 49.99 * 1.08 = 53.99
```

### 🧹 Clean Up

```sql
DROP TEMPORARY MACRO IF EXISTS double_it;
DROP TEMPORARY MACRO IF EXISTS full_name;
DROP TEMPORARY MACRO IF EXISTS tax_amount;
DROP TEMPORARY MACRO IF EXISTS status_label;
DROP TEMPORARY MACRO IF EXISTS fiscal_quarter;
DROP TEMPORARY MACRO IF EXISTS safe_div;
DROP TEMPORARY MACRO IF EXISTS cents_to_dollars;
DROP TEMPORARY MACRO IF EXISTS with_tax;
```

## ⚠️ Limitations

1. **Session-scoped only** — cannot create permanent macros; they must be recreated each session.
2. **SQL expressions only** — no procedural logic, loops, or variable assignments.
3. **No overloading** — cannot define two macros with the same name but different signatures.
4. **No recursion** — a macro cannot call itself.
5. **Spark 3.0+** — not available in older Spark versions.

## 🧠 Macros vs UDFs vs Views

| Feature | SQL Macro | UDF | View |
|---------|-----------|-----|------|
| Expansion | Inline at compile time | Executed at runtime | Subquery at compile time |
| Performance | Fastest (no overhead) | Slower (serialization) | Good (optimized) |
| Parameterized | ✅ Typed parameters | ✅ Any language | ❌ No parameters |
| Scope | Session only | Session or permanent | Session or permanent |
| Language | SQL expressions | Python, Scala, Java | SQL queries |
| Catalyst optimization | Fully optimized | Opaque to optimizer | Fully optimized |
| Use case | Reusable calculations | Complex / procedural logic | Reusable queries |

> **Tip:** Use macros for small, frequently-used calculations (tax, formatting, rounding).
> Use UDFs when you need procedural logic or external libraries. Use views when you need
> reusable query results rather than expressions.
