# :material-eye: Views

A **view** is a named SQL query stored in the catalog. Querying a view re-executes
its underlying SQL against the current data — no copy of the data is made
(unless it is a Materialized View).

---

## :material-sitemap: View Type Taxonomy

```mermaid
flowchart TD
    V["Views"] --> TV["Temporary View\nSession-scoped\nno metastore entry"]
    V --> GTV["Global Temp View\nApplication-scoped\nglobal_temp.name"]
    V --> PV["Permanent View\nMetastore-persisted\ndatabase.view_name"]
    V --> MV["Materialized View\nPrecomputed + stored\nDelta Lake / Databricks SQL"]
    PV --> UC["Unity Catalog\ncatalog.schema.view"]
    PV --> HIVE["Hive Metastore\ndatabase.view"]
```

---

## :material-compare: View Types at a Glance

| Type | Scope | Stored in catalog | Data stored | Access syntax |
|------|-------|:-----------------:|:-----------:|---------------|
| Temp view | Session | No | No | `view_name` |
| Global temp view | Spark app | No (global_temp) | No | `global_temp.view_name` |
| Permanent view | Forever | Yes | No | `db.view_name` |
| Materialized view | Forever | Yes | Yes (Delta) | `catalog.schema.view_name` |

---

## :material-lightbulb: Decision Guide

```mermaid
flowchart TD
    Q1{"Needs to survive\nthe session?"}
    Q1 -->|No| Q2{"Shared across\nnotebooks in same app?"}
    Q2 -->|No| TV["TEMP VIEW\nfastest to create, no cleanup needed"]
    Q2 -->|Yes| GTV["GLOBAL TEMP VIEW\nglobal_temp.view_name"]
    Q1 -->|Yes| Q3{"Frequently queried,\nheavy computation?"}
    Q3 -->|No| PV["Permanent VIEW\nlogic reuse, security, governance"]
    Q3 -->|Yes| MV["MATERIALIZED VIEW\nprecomputed result, Delta-backed"]
```

---

## :material-alert-circle: View vs Table vs CTE

| Feature | View | CTE (`WITH`) | Temp Table (`CTAS`) |
|---------|------|:---:|:---:|
| Stores data | No | No | Yes |
| Persists across queries | Yes (perm) | No | Session / job |
| Queryable by name | Yes | No | Yes |
| Supports predicate pushdown | Yes | Yes | Yes (Parquet) |
| Can index / OPTIMIZE | No | No | Yes (Delta) |
| Best for | Logic reuse, security | Single-query decomposition | Materialized intermediate |

---

## :material-book-open-variant: In This Section

| Page | Contents |
|------|----------|
| [View Overview](view.md) | Lifecycle, DDL commands, behavior notes |
| [Types](types.md) | Temp, global temp, permanent, materialized — full syntax and behavior |
| [Syntax](syntax.md) | Complete DDL reference for all view types |
| [Examples](example.md) | Real-world patterns — layered views, security views, rollup views |
| [FAQ](faq.md) | Common errors and troubleshooting |
