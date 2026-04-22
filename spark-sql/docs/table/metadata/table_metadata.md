# :material-information-outline: Table Metadata

Table metadata includes schema, partition columns, storage format, and table
properties. Spark exposes metadata via `DESCRIBE` and `SHOW` commands.

---

## :material-pin: Common Commands

```sql
DESCRIBE TABLE EXTENDED sales;
SHOW TBLPROPERTIES sales;
SHOW PARTITIONS sales;
```

---

## :material-magnify: Behavior Notes

1. `DESCRIBE TABLE EXTENDED` includes provider, location, and properties.
2. `SHOW PARTITIONS` works for partitioned tables only.
3. Properties can be used to store custom metadata.

---

## :material-flask-outline: Practical Example

```sql
DESCRIBE TABLE EXTENDED orders;
```

---

## :material-brain: When to Use

| Scenario | Command |
|----------|---------|
| Inspect schema | `DESCRIBE TABLE` |
| View storage details | `DESCRIBE TABLE EXTENDED` |
| List partitions | `SHOW PARTITIONS` |
| Check table props | `SHOW TBLPROPERTIES` |
