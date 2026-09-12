# :material-null: NULL Semantics

These pages document NULL behavior rechecked against PySpark 4.2.0 so the examples match real Spark SQL results.

______________________________________________________________________

## :material-view-grid: In This Section

| Page                        | Focus                                                     |
| --------------------------- | --------------------------------------------------------- |
| [Comparison](comparison.md) | Standard comparisons, `<=>`, and `IS [NOT] DISTINCT FROM` |
| [Logical](logical.md)       | Three-valued logic for `AND`, `OR`, and `NOT`             |
| [Filter](filter.md)         | Why `WHERE`, `HAVING`, and `JOIN ON` keep only `TRUE`     |
| [Aggregate](aggregate.md)   | Which aggregates ignore NULLs and which do not            |
| [Expression](expression.md) | Null-intolerant expressions and null-aware helpers        |
| [Check](check.md)           | `IS NULL`, `COALESCE`, `NULLIF`, `IFNULL`, `NVL`, `NVL2`  |
| [Operator](operator.md)     | `GROUP BY`, `DISTINCT`, and partitioning behavior         |
| [Ordering](ordering.md)     | Default and explicit NULL placement in `ORDER BY`         |
| [Sets](sets.md)             | NULL handling in `UNION`, `INTERSECT`, and `EXCEPT`       |
| [Subquery](subquery.md)     | `EXISTS`, `IN`, and the `NOT IN` NULL trap                |

______________________________________________________________________

## :material-sitemap: Verified Rules

| Topic             | Verified Spark 4.2 behavior                                                                                        |
| ----------------- | ------------------------------------------------------------------------------------------------------------------ |
| Comparisons       | `NULL = NULL` returns `NULL`, not `FALSE`; use `IS NULL`, `<=>`, or `IS NOT DISTINCT FROM` when needed             |
| Logical operators | `FALSE AND NULL` returns `FALSE`, `TRUE OR NULL` returns `TRUE`, and `NOT NULL` returns `NULL`                     |
| Aggregates        | `SUM`, `AVG`, `MIN`, `MAX`, and `COUNT(col)` ignore NULL inputs; `COUNT(*)` counts rows even when columns are NULL |
| Ordering          | `ORDER BY col ASC` defaults to `NULLS FIRST`; `ORDER BY col DESC` defaults to `NULLS LAST`                         |
| Set operations    | `UNION`, `INTERSECT`, and `EXCEPT` compare rows null-safely for set membership                                     |
| Subqueries        | `NOT IN` with a NULL in the subquery result keeps no rows; `NOT EXISTS` does not have that trap                    |

______________________________________________________________________

## :material-lightbulb-on-outline: Reading Path

Start with [Comparison](comparison.md) and [Logical](logical.md), then move to [Filter](filter.md) and [Subquery](subquery.md) to see how those rules affect real queries.
