# :material-select-group: Subqueries

Use correlated subqueries, EXISTS anti-joins, and scalar subqueries to filter and enrich result sets.

______________________________________________________________________

## :material-sitemap: Overview

```mermaid
graph LR
    A[Outer row] --> B[Correlated subquery]
    B -->|references outer.col| C[Scalar / boolean result]
    C --> D[Filter or value in outer query]
```

______________________________________________________________________

## :material-pin: Quick Reference

| Technique           | Use Case                                  | Key Function                                            |
| ------------------- | ----------------------------------------- | ------------------------------------------------------- |
| Correlated subquery | Per-row inner query referencing outer row | `WHERE col = (SELECT ... FROM t WHERE t.id = outer.id)` |
| EXISTS              | Check for the presence of matching rows   | `WHERE EXISTS (SELECT 1 FROM ...)`                      |
| NOT EXISTS          | Anti-join pattern — rows with no match    | `WHERE NOT EXISTS (SELECT 1 FROM ...)`                  |
| Subquery in WHERE   | Filter outer result on inner aggregation  | `WHERE col > (SELECT AVG(...) FROM ...)`                |
| Aggregated subquery | Scalar value from inner aggregation       | `(SELECT SUM(...) FROM ...)`                            |
| Percentage of total | Subquery in SELECT list for ratio         | `col / (SELECT SUM(col) FROM ...)`                      |

______________________________________________________________________

## :material-magnify: Examples

### Correlated Basics

Simple correlated subquery referencing the outer query's current row.

```sql
--8<-- "sql/application/subquery/correlated_basics.sql"
```

______________________________________________________________________

### Correlated Advanced

Advanced correlated patterns including EXISTS and NOT EXISTS.

```sql
--8<-- "sql/application/subquery/correlated_advanced.sql"
```

______________________________________________________________________

### Subquery Filtering

Use subqueries in WHERE to filter rows based on inner aggregations.

```sql
--8<-- "sql/application/subquery/subquery_filtering.sql"
```

______________________________________________________________________

### Subquery Aggregation

Scalar subqueries in the SELECT list for ratio and percentage calculations.

```sql
--8<-- "sql/application/subquery/subquery_aggregation.sql"
```

______________________________________________________________________

## :material-database-search: [Databricks] Real-World Example — System Tables

!!! note "[Databricks] Unity Catalog system tables"

    `system.billing.usage` is a built-in Unity Catalog system table, so no sample data
    creation step is required. An account admin must `GRANT USE CATALOG, USE SCHEMA, SELECT ON SCHEMA system.billing TO <principal>` before these queries will return
    rows.

### Workspace totals filtered against the account-wide average

```sql
-- [Databricks] Requires SELECT on system.billing.usage
SELECT
    workspace_id,
    SUM(usage_quantity) AS total_quantity,
    ROUND(
        SUM(usage_quantity) * 100.0 /
        (SELECT SUM(usage_quantity)
         FROM system.billing.usage
         WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 30)),
        2
    ) AS pct_of_account_usage
FROM system.billing.usage
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY workspace_id
HAVING SUM(usage_quantity) > (
    SELECT AVG(workspace_total)
    FROM (
        SELECT
            workspace_id,
            SUM(usage_quantity) AS workspace_total
        FROM system.billing.usage
        WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 30)
        GROUP BY workspace_id
    ) AS workspace_totals
)
ORDER BY total_quantity DESC;
-- Result (illustrative):
-- workspace_id | total_quantity | pct_of_account_usage
-- ------------|----------------|---------------------
-- 123456789   | 5538.60        | 24.18
-- 987654321   | 4210.35        | 18.39
```

!!! tip "Scalar subqueries scale too"

    The same subquery-in-`HAVING` and subquery-in-`SELECT` patterns used on toy data
    are useful for real chargeback analysis when you need workspace metrics compared
    with an account-level baseline.

______________________________________________________________________

## :material-brain: When to Use

| Scenario                              | Recommended Approach    |
| ------------------------------------- | ----------------------- |
| Per-row aggregation against outer row | Correlated subquery     |
| Anti-join (rows with no match)        | `NOT EXISTS`            |
| Scalar lookup in SELECT               | Subquery in SELECT list |
| Filter on an aggregated value         | Subquery in WHERE       |

!!! tip

    For large datasets, prefer JOIN + aggregation or window functions over correlated subqueries — they typically have better query plans.
