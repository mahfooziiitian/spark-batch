# :material-check-decagram: ANSI Mode

!!! warning "Breaking Change in Spark 4.0"

    `spark.sql.ansi.enabled` is **`true` by default** in Spark 4.0.
    Previously it was `false`.

ANSI mode enforces SQL standard behavior for type casting, arithmetic overflow,
and invalid operations — raising errors instead of silently returning NULL.

### :material-animation-play: Interactive Visualization — Error vs NULL Semantics

Toggle ANSI mode and compare the same expressions under strict and permissive behavior.
The third view shows how `try_*` functions localize NULL-on-error handling without
disabling ANSI for the whole session.

<div id="viz-config-ansi" class="ts-viz"></div>

<script src="../assets/js/configuration-viz.js"></script>

______________________________________________________________________

## :material-pin: What Changes

### Arithmetic Overflow

```sql
-- Spark 3.x: returned wrapped value silently
-- Spark 4.0: throws SparkArithmeticException
SELECT 2147483647 + 1;
-- Error: [ARITHMETIC_OVERFLOW] integer overflow

-- Safe alternative: returns NULL on overflow
SELECT try_add(2147483647, 1);  -- NULL
```

### Invalid Type Casting

```sql
-- Spark 3.x: CAST('abc' AS INT) = NULL
-- Spark 4.0: throws SparkNumberFormatException
SELECT CAST('abc' AS INT);

-- Safe alternative
SELECT try_cast('abc' AS INT);  -- NULL
```

### Invalid Store Assignment

```sql
CREATE TABLE t(v INT);
-- Spark 4.0: AnalysisException for invalid type
INSERT INTO t VALUES ('not_a_number');
```

______________________________________________________________________

## :material-compare: Error vs NULL Behavior

| Expression                          | ANSI `true`                     | ANSI `false`           | Safe alternative                 |
| ----------------------------------- | ------------------------------- | ---------------------- | -------------------------------- |
| `2147483647 + 1`                    | Error (`ARITHMETIC_OVERFLOW`)   | Wrapped integer result | `try_add(...)` → `NULL`          |
| `CAST('abc' AS INT)`                | Error (`NumberFormatException`) | `NULL`                 | `try_cast(...)` → `NULL`         |
| `to_timestamp('bad', 'yyyy-MM-dd')` | Error                           | `NULL`                 | `try_to_timestamp(...)` → `NULL` |

The important migration pattern is to keep ANSI mode **on** globally for better data
quality, then use `try_*` functions only where permissive parsing is intentional.

______________________________________________________________________

## :material-shield-half-full: ANSI-Safe Function Variants

Use `try_*` functions to get NULL-on-error behavior (matching Spark 3.x defaults):

| ANSI Function     | `try_*` Safe Variant  | On Error |
| ----------------- | --------------------- | -------- |
| `a + b`           | `try_add(a, b)`       | NULL     |
| `a - b`           | `try_subtract(a, b)`  | NULL     |
| `a * b`           | `try_multiply(a, b)`  | NULL     |
| `a / b`           | `try_divide(a, b)`    | NULL     |
| `CAST(x AS T)`    | `try_cast(x AS T)`    | NULL     |
| `to_date(s)`      | `try_to_date(s)`      | NULL     |
| `to_timestamp(s)` | `try_to_timestamp(s)` | NULL     |
| `to_time(s)`      | `try_to_time(s)`      | NULL     |

______________________________________________________________________

## :material-code-tags: Examples

```sql
-- Arithmetic with safe alternatives
SELECT
    try_add(price, tax) AS total,                -- NULL on overflow
    try_divide(revenue, units) AS unit_price    -- NULL on divide-by-zero
FROM sales;

-- Safe casting
SELECT
    try_cast(raw_value AS INT) AS parsed_int,
    try_cast(raw_value AS DOUBLE) AS parsed_double
FROM raw_data;

-- Safe date parsing
SELECT
    try_to_date(date_str, 'yyyy-MM-dd') AS parsed_date,
    try_to_timestamp(ts_str, 'yyyy-MM-dd HH:mm') AS parsed_ts
FROM imports;
```

______________________________________________________________________

## :material-restore: Restoring Spark 3.x Behavior

```sql
-- Disable ANSI mode for the session
SET spark.sql.ansi.enabled = false;
```

Or when launching the SQL shell or an application:

```bash
spark-sql --conf spark.sql.ansi.enabled=false
```

Or in `spark-defaults.conf`:

```properties
spark.sql.ansi.enabled=false
```

______________________________________________________________________

## :material-lightbulb-outline: Migration Tips

1. **Audit existing queries** for implicit NULL-on-error behavior.
2. **Replace** `CAST(x AS T)` with `try_cast(x AS T)` where NULL is expected.
3. **Replace** arithmetic operators with `try_*` variants for nullable columns.
4. **Test data pipelines** — previously silent failures now raise explicit errors.
5. Keep ANSI mode **on** by default unless you are deliberately emulating legacy behavior.
