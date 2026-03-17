# Bucketing in Hive Tables

Bucketing divides data into fixed hash buckets. It can improve join performance
when tables are bucketed on the same key.

---

## 📌 Syntax

```sql
CREATE TABLE bucketed_sales (
  id BIGINT,
  amount DOUBLE
) USING PARQUET
CLUSTERED BY (id) INTO 32 BUCKETS;
```

---

## 🔍 Behavior

1. Bucketing writes data into fixed bucket files.
2. Joins on bucketed keys can avoid shuffles in some cases.
3. Requires consistent bucketing across tables.

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Large join tables | Use bucketing on join keys |
| Small tables | Bucketing not necessary |
