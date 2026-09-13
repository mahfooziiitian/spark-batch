# :material-table-arrow-right: Derived Tables

Use inline subqueries in the FROM clause as intermediate aggregation layers.

______________________________________________________________________

## :material-sitemap: Overview

```mermaid
graph LR
    A[Outer query] --> B[(subquery in FROM)]
    B --> C[Aliased result]
    C --> D[Filtered / joined result]
```

______________________________________________________________________

## :material-pin: Quick Reference

| Technique                    | Use Case                       | Key Function                       |
| ---------------------------- | ------------------------------ | ---------------------------------- |
| Subquery in FROM             | Intermediate calculation layer | `FROM (SELECT ...) AS alias`       |
| Grouping with classification | Custom bucket assignment       | `CASE` inside derived table        |
| JOIN derived tables          | Combine two subquery results   | `JOIN (SELECT ...) AS alias`       |
| Year-on-year comparison      | Multi-derived join on period   | Two derived tables joined          |
| Multi-level aggregation      | Nested grouping                | Derived table inside derived table |
| Synchronised filters         | Shared WHERE predicate         | Same filter in multiple subqueries |

______________________________________________________________________

## :material-magnify: Examples

### Derived Table Basics

Subquery in FROM for intermediate grouping before outer filtering.

```sql
--8<-- "sql/application/derived_table/derived_table_basics.sql"
```

______________________________________________________________________

### Derived Table Advanced

Year-on-year comparison and multi-level aggregation with joined derived tables.

```sql
--8<-- "sql/application/derived_table/derived_table_advanced.sql"
```

______________________________________________________________________

## :material-database-search: [Databricks] Real-World Example — System Tables

!!! note "[Databricks] Unity Catalog system tables"

    `system.billing.usage` is a built-in Unity Catalog system table, so the query can
    run directly against real usage data. An account admin must `GRANT USE CATALOG, USE SCHEMA, SELECT ON SCHEMA system.billing TO <principal>` before it will return
    rows.

### Derived table for daily workspace rollups

```sql
-- [Databricks] Requires SELECT on system.billing.usage
SELECT
    daily_usage.workspace_id,
    daily_usage.avg_daily_quantity,
    daily_usage.peak_daily_quantity
FROM (
    SELECT
        workspace_id,
        ROUND(AVG(daily_quantity), 2) AS avg_daily_quantity,
        ROUND(MAX(daily_quantity), 2) AS peak_daily_quantity
    FROM (
        SELECT
            workspace_id,
            usage_date,
            SUM(usage_quantity) AS daily_quantity
        FROM system.billing.usage
        WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 30)
        GROUP BY workspace_id, usage_date
    ) AS per_day
    GROUP BY workspace_id
) AS daily_usage
WHERE daily_usage.peak_daily_quantity > daily_usage.avg_daily_quantity * 1.5
ORDER BY daily_usage.peak_daily_quantity DESC;
-- Result (illustrative):
-- workspace_id | avg_daily_quantity | peak_daily_quantity
-- ------------|--------------------|--------------------
-- 123456789   | 184.62             | 392.40
-- 555555555   | 71.80              | 133.10
```

!!! tip "Inline aggregation layers"

    A derived table is useful when you only need an intermediate aggregate once. The
    pattern scales directly from toy sales examples to production billing rollups on
    `system.billing.usage`.

______________________________________________________________________

## :material-brain: When to Use

| Scenario                       | Recommended Approach                |
| ------------------------------ | ----------------------------------- |
| Intermediate group then filter | Derived table in FROM               |
| Year-over-year comparison      | Two derived tables joined on period |
| Multi-level aggregation        | Nested derived tables               |

!!! tip

    Prefer CTEs over derived tables for readability when the subquery is referenced more than once.
