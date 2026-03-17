# Table Metadata

Table metadata includes schema, partition columns, storage format, and table
properties. Spark exposes metadata via `DESCRIBE` and `SHOW` commands.

---

## 📌 Common Commands

```sql
DESCRIBE TABLE EXTENDED sales;
SHOW TBLPROPERTIES sales;
SHOW PARTITIONS sales;
```

---

## 🔍 Behavior Notes

1. `DESCRIBE TABLE EXTENDED` includes provider, location, and properties.
2. `SHOW PARTITIONS` works for partitioned tables only.
3. Properties can be used to store custom metadata.

---

## 🧪 Practical Example

```sql
DESCRIBE TABLE EXTENDED orders;
```

---

## 🧠 When to Use

| Scenario | Command |
|----------|---------|
| Inspect schema | `DESCRIBE TABLE` |
| View storage details | `DESCRIBE TABLE EXTENDED` |
| List partitions | `SHOW PARTITIONS` |
| Check table props | `SHOW TBLPROPERTIES` |
