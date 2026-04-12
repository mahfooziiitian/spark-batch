# :material-bee: Hive Catalog

The Hive catalog uses Hive Metastore to store Spark SQL table metadata.
It is commonly exposed as `spark_catalog` when Spark is configured with Hive
support.

---

## 📌 Key Features

| Feature | Description |
|---------|-------------|
| Backed by | Hive Metastore |
| Storage | Tracks table schemas and locations |
| Compatibility | Works with Hive, Spark SQL |

---

## 🧪 Practical Examples

```sql
SHOW DATABASES;
USE default;

CREATE TABLE hive_sales (
  id BIGINT,
  amount DOUBLE
) USING PARQUET;
```

---

## 🔍 Behavior Notes

1. The Hive catalog is persistent across sessions.
2. Tables can be **managed** or **external**.
3. Hive Metastore may require a separate service (Thrift) in some deployments.

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Legacy Hive integration | Use Hive catalog |
| Shared metadata store | Hive Metastore |
| Modern governance | Unity Catalog (if available) |
