# :material-table-split-cell: Partitioned Managed Tables

Partitioned managed tables store data in the warehouse directory and are
physically organized by partition columns.

---

## 📌 Syntax

```sql
CREATE TABLE sales (
  order_id BIGINT,
  order_date DATE,
  amount DOUBLE
) USING PARQUET
PARTITIONED BY (order_date);
```

---

## 🔍 Behavior

1. Spark manages data files under the warehouse path.
2. Partitions are represented as folder paths (e.g., `order_date=2024-01-01`).
3. Dropping the table removes both metadata and data files.

---

## 🧪 Example

```sql
INSERT INTO sales
SELECT * FROM staging_sales;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Managed lifecycle | Use managed tables |
| Partition pruning | Partition by filter columns |
| Data lake sharing | Use external tables |
