# Session Catalog

The **session catalog** stores session-scoped objects such as temporary views
and temporary functions. These objects exist only for the current Spark session
and are not persisted in the metastore.

---

## 📌 Key Characteristics

| Feature | Behavior |
|---------|----------|
| Scope | Current Spark session only |
| Persistence | Not persisted across restarts |
| Typical objects | Temp views, temp functions |
| Namespace | `session` (implicit) |

---

## 🧪 Practical Examples

### Create a Temporary View

```sql
CREATE OR REPLACE TEMP VIEW recent_orders AS
SELECT * FROM orders
WHERE order_date >= current_date() - 7;
```

### Use the Temp View

```sql
SELECT * FROM recent_orders WHERE amount > 100;
```

### Drop a Temp View

```sql
DROP VIEW IF EXISTS recent_orders;
```

---

## 🔍 Behavior Notes

1. Temp views are **session-scoped** and disappear when the session ends.
2. Temp views can shadow permanent tables with the same name.
3. Temp views are ideal for intermediate steps in notebooks and pipelines.

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Ad-hoc analysis | Use temp views for intermediate results |
| Multi-step SQL workflows | Create temp views between steps |
| Persistent datasets | Use managed or external tables instead |
