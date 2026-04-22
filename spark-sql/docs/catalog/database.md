# :material-database: Databases (Schemas)

Databases (schemas) are logical namespaces that group tables and views.

---

## :material-pin: Common Commands

```sql
SHOW DATABASES;
CREATE DATABASE analytics;
USE analytics;
```

---

## :material-magnify: Behavior Notes

1. `DATABASE` and `SCHEMA` are interchangeable in Spark SQL.
2. `USE` changes the current database context.
3. Dropping a database can remove contained objects.

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Organize domains | Create schemas |
| Separate environments | Use dev/test/prod schemas |
