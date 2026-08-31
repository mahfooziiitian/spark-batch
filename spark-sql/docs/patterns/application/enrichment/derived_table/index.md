# :material-table-arrow-right: Derived Tables

Use inline subqueries in the FROM clause as intermediate aggregation layers.

---

## :material-sitemap: Overview

```mermaid
graph LR
    A[Outer query] --> B[(subquery in FROM)]
    B --> C[Aliased result]
    C --> D[Filtered / joined result]
```

---

## :material-pin: Quick Reference

| Technique | Use Case | Key Function |
|-----------|----------|-------------|
| Subquery in FROM | Intermediate calculation layer | `FROM (SELECT ...) AS alias` |
| Grouping with classification | Custom bucket assignment | `CASE` inside derived table |
| JOIN derived tables | Combine two subquery results | `JOIN (SELECT ...) AS alias` |
| Year-on-year comparison | Multi-derived join on period | Two derived tables joined |
| Multi-level aggregation | Nested grouping | Derived table inside derived table |
| Synchronised filters | Shared WHERE predicate | Same filter in multiple subqueries |

---

## :material-magnify: Examples

### Derived Table Basics

Subquery in FROM for intermediate grouping before outer filtering.

```sql
--8<-- "sql/application/derived_table/derived_table_basics.sql"
```

---

### Derived Table Advanced

Year-on-year comparison and multi-level aggregation with joined derived tables.

```sql
--8<-- "sql/application/derived_table/derived_table_advanced.sql"
```

---

## :material-brain: When to Use

| Scenario | Recommended Approach |
|----------|---------------------|
| Intermediate group then filter | Derived table in FROM |
| Year-over-year comparison | Two derived tables joined on period |
| Multi-level aggregation | Nested derived tables |

!!! tip
    Prefer CTEs over derived tables for readability when the subquery is referenced more than once.
