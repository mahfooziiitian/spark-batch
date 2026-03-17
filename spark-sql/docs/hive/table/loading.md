# Loading Hive Tables

Hive tables can be loaded from files or inserted using Spark SQL.

---

## 📌 Common Methods

| Method | Example |
|--------|---------|
| `LOAD DATA` | Load files into table location |
| `INSERT INTO` | Append rows from a query |
| `INSERT OVERWRITE` | Replace data |

---

## 🧪 Examples

```sql
LOAD DATA INPATH 's3://data/sales/' INTO TABLE hive_sales;
```

```sql
INSERT INTO hive_sales
SELECT * FROM staging_sales;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Bulk load from files | `LOAD DATA` |
| Transform and load | `INSERT INTO ... SELECT` |
| Replace partitions | `INSERT OVERWRITE` |
