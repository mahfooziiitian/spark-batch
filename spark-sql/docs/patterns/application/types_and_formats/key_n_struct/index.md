# :material-key-variant: Keys & Structs

Check for dynamic key existence in MAP columns using MAP_ENTRIES and EXISTS on Databricks system tables.

______________________________________________________________________

## :material-sitemap: Overview

```mermaid
graph LR
    A[MAP column] --> B[MAP_ENTRIES]
    B --> C[EXISTS check]
    C --> D[Filtered rows]
```

______________________________________________________________________

## :material-pin: Quick Reference

| Technique           | Use Case                                             | Key Function                    |
| ------------------- | ---------------------------------------------------- | ------------------------------- |
| MAP_ENTRIES         | Iterate over map key-value pairs as array of structs | `MAP_ENTRIES(map_col)`          |
| EXISTS(array, pred) | Test if any element matches a predicate              | `EXISTS(arr, x -> condition)`   |
| FILTER              | Keep only matching map entries                       | `FILTER(MAP_ENTRIES(col), ...)` |

______________________________________________________________________

## :material-magnify: Examples

### Any Key Exists in Struct

Check whether any key in a MAP column matches a runtime predicate — demonstrated on the Databricks `system.billing.usage` system table.

```sql
--8<-- "sql/application/key_n_struct/any_key_exists_in_struct.sql"
```

______________________________________________________________________

## :material-database-search: [Databricks] Real-World Example — System Tables

!!! note "[Databricks] Unity Catalog system tables"

    `system.billing.usage` and `system.access.audit` both expose nested data types:
    `usage_metadata` and `user_identity` are `STRUCT`s, while `custom_tags` and
    `request_params` are `MAP`s. An account admin must grant `SELECT` on the relevant
    `system` schema before these queries will return rows.

### Accessing STRUCT and MAP fields directly

```sql
-- [Databricks] Requires SELECT on system.billing.usage
SELECT
    workspace_id,
    usage_date,
    usage_metadata.cluster_id AS cluster_id,
    usage_metadata.job_id AS job_id,
    custom_tags['Tenant'] AS tenant,
    custom_tags['Env'] AS env,
    usage_quantity
FROM system.billing.usage
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 7)
  AND custom_tags['Tenant'] IS NOT NULL;
-- Result (illustrative):
-- workspace_id | usage_date  | cluster_id | job_id | tenant  | env  | usage_quantity
-- ------------|-------------|------------|--------|---------|------|---------------
-- 123456789   | 2024-07-02  | 0312-...   | 998877 | acme-co | prod | 14.25
```

### Filtering audit records by nested identity fields

```sql
-- [Databricks] Requires SELECT on system.access.audit
SELECT
    event_time,
    user_identity.email AS user_email,
    request_params['path'] AS request_path,
    response.status_code AS status_code
FROM system.access.audit
WHERE event_date >= DATE_SUB(CURRENT_DATE(), 1)
  AND user_identity.email LIKE '%@example.com';
-- Result (illustrative):
-- event_time           | user_email           | request_path           | status_code
-- ---------------------|----------------------|------------------------|------------
-- 2024-07-02 09:41:03  | analyst@example.com  | /api/2.0/sql/queries   | 200
```

!!! tip "Nested fields are first-class columns"

    Production system tables are full of maps and structs. Once you know dot notation
    and map-key lookup syntax, the same technique works for billing tags, audit
    identities, request parameters, and many other nested operational datasets.

______________________________________________________________________

## :material-brain: When to Use

| Scenario                                   | Recommended Approach            |
| ------------------------------------------ | ------------------------------- |
| Check if any key exists in a map           | `EXISTS` + `MAP_ENTRIES`        |
| Databricks system tables with MAP columns  | `system.billing.usage` pattern  |
| Dynamic key check without knowing key name | Runtime predicate with `EXISTS` |

!!! note

    MAP_ENTRIES returns an array of structs. Combine with EXISTS to check dynamic keys without knowing the key name at query-write time.
