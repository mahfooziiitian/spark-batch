# :material-math-integral: SQL Operators

Spark SQL operators shape expressions, filters, joins, and result-set composition. This section focuses on operator behavior verified against PySpark 4.2, including the important default that `spark.sql.ansi.enabled = true`.

## :material-animation-play: Interactive Visualization — Operator Family Map

<div id="viz-operator-overview" class="ts-viz"></div>

Use the selector to jump between operator families and see the key behavior each page verifies in Spark 4.2.

<script src="../../../assets/js/querying-operator-viz.js"></script>

______________________________________________________________________

## :material-sitemap: In This Section

| Page                        | Covers                                                                     |
| --------------------------- | -------------------------------------------------------------------------- |
| [Arithmetic](arithmetic.md) | `+`, `-`, `*`, `/`, `%`, `DIV`, overflow, divide-by-zero, ANSI vs non-ANSI |
| [Bitwise](bitwise.md)       | `&`, `\|`, `^`, `~`, shifts, integral-type requirements                    |
| [Comparison](comparison.md) | `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`, `BETWEEN`, `LIKE`, `ILIKE`, `RLIKE` |
| [Logical](logical.md)       | `AND`, `OR`, `NOT`, precedence, three-valued logic                         |
| [Null-Safe](null-safe.md)   | `<=>`, `IS NULL`, `IS DISTINCT FROM`, `COALESCE`, `NULLIF`                 |
| [Set](set.md)               | `UNION`, `UNION ALL`, `INTERSECT`, `INTERSECT ALL`, `EXCEPT`, `EXCEPT ALL` |
| [String](string.md)         | \`                                                                         |

______________________________________________________________________

## :material-table: Spark 4.2 Highlights Verified for This Section

| Topic                        | Verified behavior                                                                      |
| ---------------------------- | -------------------------------------------------------------------------------------- |
| ANSI mode default            | `spark.sql.ansi.enabled` is `true` by default in PySpark 4.2                           |
| Arithmetic overflow          | Raises `ARITHMETIC_OVERFLOW` by default; wraps only when ANSI is disabled              |
| Division / remainder by zero | Raises by default for `/`, `DIV`, `%`, and `MOD`; returns `NULL` when ANSI is disabled |
| Comparison with `NULL`       | Standard comparisons return `NULL`; `<=>` and `IS [NOT] DISTINCT FROM` stay boolean    |
| Bitwise operators            | Work on integral types; decimal / floating-point operands fail analysis                |
| Set operators                | Non-`ALL` forms deduplicate; `ALL` forms preserve duplicate counts                     |
| String concatenation         | \`                                                                                     |

!!! warning "ANSI mode is a per-warehouse setting on Databricks SQL"

    `spark.sql.ansi.enabled` is `true` by default in open-source PySpark 4.2, but **Databricks SQL
    warehouses can be configured with a different default**. Verified directly on a Databricks SQL
    warehouse (`stg` profile): `SET -v` reported `ansi_mode = false`, `2147483647 + 1` returned the
    wrapped value `-2147483648` instead of raising `ARITHMETIC_OVERFLOW`, and `1 / 0` returned `NULL`
    instead of raising `DIVIDE_BY_ZERO`. `SET spark.sql.ansi.enabled = false` also failed with
    `CONFIG_NOT_AVAILABLE` — on Databricks SQL, this setting is fixed at the warehouse level and is
    not adjustable per session. Before relying on ANSI-strict behavior in a Databricks SQL notebook
    or query, check the effective value with `SET -v` (filter for `ansi_mode`) rather than assuming
    the OSS Spark default.

______________________________________________________________________

## :material-information-outline: Operator Precedence Snapshot

These precedence relationships were checked directly in Spark SQL expressions:

| Higher precedence              | Lower precedence                                         |
| ------------------------------ | -------------------------------------------------------- |
| Arithmetic before comparison   | `1 + 2 > 2` evaluates arithmetic first                   |
| Comparison before `AND` / `OR` | `amount > 100 AND status = 'open'`                       |
| `NOT` before `AND`             | `NOT TRUE AND FALSE` evaluates as `(NOT TRUE) AND FALSE` |
| `AND` before `OR`              | `TRUE OR FALSE AND FALSE` returns `TRUE`                 |

Use parentheses whenever precedence is not obvious to the next reader.
