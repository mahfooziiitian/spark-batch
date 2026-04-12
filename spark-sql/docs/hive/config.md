# :material-bee: Hive Configuration

Hive-related Spark settings control metastore connectivity, warehouse paths,
partition behavior, and compatibility.

### :material-sitemap: Overview

```mermaid
graph LR
    A["Hive Metastore"] --> B["Spark SQL Catalog"]
    B --> C["Tables / Partitions / Views"]
    C --> D["Query Execution"]
```

---

## 📌 Common Settings

| Setting | Description |
|---------|-------------|
| `spark.sql.warehouse.dir` | Default warehouse directory |
| `hive.metastore.uris` | Thrift URIs for Hive Metastore |
| `spark.sql.catalogImplementation` | `hive` or `in-memory` |

---

## 🧪 Example

```sql
SET spark.sql.catalogImplementation = hive;
SET hive.metastore.uris = thrift://metastore:9083;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Enable Hive catalog | Set `spark.sql.catalogImplementation=hive` |
| External metastore | Set `hive.metastore.uris` |
| Custom warehouse | Configure `spark.sql.warehouse.dir` |
