# :material-plus-minus-variant: Arithmetic Operators

Spark SQL arithmetic looks familiar, but Spark 4.2's ANSI default makes overflow and divide-by-zero behavior much stricter than older Spark examples often assume.

## :material-animation-play: Interactive Visualization — ANSI Arithmetic Outcomes

<div id="viz-operator-arithmetic-ansi" class="ts-viz"></div>

Toggle ANSI mode to compare the same expression under Spark 4.2's default strict behavior and non-ANSI compatibility mode.

<script src="../../../assets/js/querying-operator-viz.js"></script>

______________________________________________________________________

## :material-code-tags: Syntax

| Operator | Name                       | Example      | Verified result   |
| -------- | -------------------------- | ------------ | ----------------- |
| `+`      | Addition                   | `10 + 3`     | `13`              |
| `-`      | Subtraction                | `10 - 3`     | `7`               |
| `*`      | Multiplication             | `10 * 3`     | `30`              |
| `/`      | Division                   | `10 / 4`     | `2.5`             |
| `%`      | Remainder                  | `10 % 3`     | `1`               |
| `DIV`    | Integer division           | `10 DIV 3`   | `3`               |
| `MOD`    | Function form of remainder | `MOD(10, 3)` | `1`               |
| `-x`     | Unary negation             | `-amount`    | Negates the value |

!!! note "Spark 4.2 default"

    In PySpark 4.2, `spark.sql.ansi.enabled` is `true` by default. That affects overflow and zero-divisor behavior immediately, even in simple `SELECT` expressions.

______________________________________________________________________

## :material-information-outline: Verified Behavior

| Expression checked in PySpark 4.2           | ANSI `true`           | ANSI `false`           |
| ------------------------------------------- | --------------------- | ---------------------- |
| `2147483647 + 1`                            | `ARITHMETIC_OVERFLOW` | `-2147483648`          |
| `CAST(9223372036854775807L AS BIGINT) + 1L` | `ARITHMETIC_OVERFLOW` | `-9223372036854775808` |
| `1 / 0`                                     | `DIVIDE_BY_ZERO`      | `NULL`                 |
| `CAST(1 AS DOUBLE) / CAST(0 AS DOUBLE)`     | `DIVIDE_BY_ZERO`      | `NULL`                 |
| `1 DIV 0`                                   | `DIVIDE_BY_ZERO`      | `NULL`                 |
| `1 % 0` / `MOD(1, 0)`                       | `REMAINDER_BY_ZERO`   | `NULL`                 |

Additional type checks verified in PySpark 4.2:

- `typeof(1 / 2)` is `double`.
- `typeof(1 DIV 2)` is `bigint`.
- `typeof(CAST(1 AS INT) + CAST(1.5 AS DOUBLE))` is `double`.
- `typeof(CAST(1 AS DECIMAL(10,2)) + CAST(2 AS DECIMAL(10,2)))` is `decimal(11,2)`.

!!! warning "Do not rely on legacy non-ANSI behavior"

    Older Spark examples often say overflow wraps and division by zero returns `NULL`. In open-source
    PySpark 4.2 that is only true after explicitly setting `spark.sql.ansi.enabled = false`.

!!! warning "Databricks SQL warehouses can default to ANSI `false`, and you cannot flip it with `SET`"

    Verified directly on a Databricks SQL warehouse (`stg` profile): `SET -v` reported `ansi_mode = false`, so `2147483647 + 1` returned `-2147483648` (wrapped) and `1 / 0`, `1 DIV 0`, and `1 % 0`
    all returned `NULL` — the "ANSI `false`" column of the table above, not the "ANSI `true`" column
    — without changing any session setting. Running `SET spark.sql.ansi.enabled = false;` on that
    same warehouse failed with `[CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION] Configuration spark.sql.ansi.enabled is not available.` ANSI mode on Databricks SQL is fixed per warehouse
    (or per notebook cluster), not adjustable with a session-level `SET` statement. Check `SET -v`
    and look for `ansi_mode` to see what a given warehouse actually does before assuming either
    default.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Default ANSI overflow

```sql
SELECT 2147483647 + 1 AS v;
-- Spark 4.2 default: ARITHMETIC_OVERFLOW
```

### Non-ANSI compatibility mode

```sql
SET spark.sql.ansi.enabled = false;

SELECT 2147483647 + 1 AS wrapped_int;
SELECT CAST(9223372036854775807L AS BIGINT) + 1L AS wrapped_bigint;
-- Results: -2147483648 and -9223372036854775808
```

!!! note "This `SET` fails on Databricks SQL warehouses"

    Verified on a Databricks SQL warehouse (`stg` profile): `SET spark.sql.ansi.enabled = false;`
    raises `[CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION] Configuration spark.sql.ansi.enabled is not available.` The setting is controlled at the warehouse level there, not per session. On that
    same warehouse, `ansi_mode` was already `false` by default (confirmed with `SET -v`), so the two
    `SELECT` statements above returned the wrapped values without needing the `SET` at all.

### Division and remainder by zero

```sql
-- ANSI true (default): all three raise
SELECT 1 / 0;
SELECT 1 DIV 0;
SELECT 1 % 0;

-- ANSI false: all three return NULL
SET spark.sql.ansi.enabled = false;
SELECT 1 / 0 AS slash_div, 1 DIV 0 AS int_div, 1 % 0 AS remainder;
```

### Choosing an explicit safe pattern

```sql
SELECT
    revenue,
    units_sold,
    revenue / NULLIF(units_sold, 0) AS revenue_per_unit
FROM sales_summary;
```

### Type promotion examples

```sql
SELECT
    CAST(1 AS INT) + CAST(1.5 AS DOUBLE) AS promoted_value,
    typeof(CAST(1 AS INT) + CAST(1.5 AS DOUBLE)) AS promoted_type,
    1 / 2 AS regular_division,
    typeof(1 / 2) AS regular_division_type,
    1 DIV 2 AS integer_division,
    typeof(1 DIV 2) AS integer_division_type;
```

### Decimal arithmetic keeps decimal precision

```sql
SELECT
    CAST(1 AS DECIMAL(10,2)) + CAST(2 AS DECIMAL(10,2)) AS sum_value,
    typeof(CAST(1 AS DECIMAL(10,2)) + CAST(2 AS DECIMAL(10,2))) AS sum_type;
-- Result type verified in PySpark 4.2: decimal(11,2)
```

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                   | Recommended pattern                       |
| -------------------------- | ----------------------------------------- |
| Integer quotient           | `a DIV b`                                 |
| Fractional quotient        | `a / b`                                   |
| Safe zero-divisor handling | `a / NULLIF(b, 0)` or `try_divide(a, b)`  |
| Safe overflow handling     | `try_add`, `try_subtract`, `try_multiply` |
| Decimal money math         | Cast to `DECIMAL(p,s)` explicitly         |

!!! tip

    If a tutorial must show non-ANSI wraparound or `NULL`-on-zero behavior, set `spark.sql.ansi.enabled = false` in the example instead of assuming it.
