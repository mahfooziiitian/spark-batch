# :material-database-export: External Catalog

An external catalog refers to a metadata store outside the Spark session that
manages table definitions and locations. Examples include Hive Metastore or
Unity Catalog.

---

## :material-pin: Why Use an External Catalog

| Benefit | Description |
|---------|-------------|
| Persistence | Metadata survives cluster restarts |
| Sharing | Multiple clusters can access the same tables |
| Governance | Central access control and lineage |

---

## :material-flask-outline: Practical Examples

### Create a Table in External Catalog

```sql
CREATE TABLE external_db.sales (
  id BIGINT,
  amount DOUBLE
) USING DELTA;
```

### Show Catalogs and Schemas

```sql
SHOW CATALOGS;
SHOW SCHEMAS IN external_db;
```

---

## :material-magnify: Behavior Notes

1. External catalogs typically manage table locations and schema evolution.
2. They allow sharing data across multiple Spark clusters.

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Shared data lake tables | Use external catalog |
| Governance and auditing | Prefer centralized catalog |
| Single-session scratch work | Use session catalog instead |
