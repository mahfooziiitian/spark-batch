# Refresh Table Metadata

`REFRESH TABLE` invalidates cached metadata and refreshes table information.
This is useful when underlying data changes outside Spark.

---

## 📌 Syntax

```sql
REFRESH TABLE table_name;
```

---

## 🔍 Behavior

1. Clears cached metadata and file listings.
2. Does not change data; it only refreshes Spark's view.
3. Often needed after external file updates.

---

## 🧪 Example

```sql
REFRESH TABLE sales;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Files updated externally | Run `REFRESH TABLE` |
| Table newly created by another engine | Refresh metadata |
| Stale query results | Refresh and re-query |
