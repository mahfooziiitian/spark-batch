# :material-unity: Unity Catalog

Unity Catalog is a centralized governance solution (Databricks) for managing
metadata, access control, and lineage across workspaces. It introduces a
three-level namespace: **catalog.schema.table**.

---

## :material-pin: Core Concepts

| Concept | Description |
|---------|-------------|
| Catalog | Top-level container (e.g., `sales_catalog`) |
| Schema | Database-like container within a catalog |
| Table | Managed or external table |
| Volume | Managed storage location |

---

## :material-flask-outline: Practical Examples

### Create and Use a Catalog

```sql
CREATE CATALOG IF NOT EXISTS sales_catalog;
CREATE SCHEMA IF NOT EXISTS sales_catalog.analytics;

CREATE TABLE sales_catalog.analytics.orders (
  order_id BIGINT,
  amount DOUBLE
) USING DELTA;
```

### Grant Access

```sql
GRANT SELECT ON TABLE sales_catalog.analytics.orders TO `analyst_role`;
```

---

## :material-magnify: Behavior Notes

1. Unity Catalog enforces **centralized access control** via grants.
2. Object names are fully qualified: `catalog.schema.table`.
3. Supports lineage tracking and fine-grained permissions.

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Multi-workspace governance | Use Unity Catalog |
| Central permissions | Grant at catalog/schema/table level |
| Data lineage needs | Enable Unity Catalog lineage features |
