# List Operations

In Spark SQL, lists are represented as `ARRAY` types. The primary functions for building
lists from grouped rows are `COLLECT_LIST` and `COLLECT_SET`.

## 📌 COLLECT_LIST — Aggregate with Duplicates

```sql
SELECT COLLECT_LIST(col) FROM VALUES (1), (2), (1) AS tab(col);
-- Result: [1, 2, 1]
```

## 📌 COLLECT_SET — Aggregate without Duplicates

```sql
SELECT COLLECT_SET(col) FROM VALUES (1), (2), (1) AS tab(col);
-- Result: [1, 2]
```

## 🧪 Grouped Collection

```sql
CREATE OR REPLACE TEMP VIEW purchases AS
SELECT * FROM VALUES
  ('Alice', 'laptop'), ('Alice', 'mouse'), ('Alice', 'laptop'),
  ('Bob', 'keyboard'), ('Bob', 'monitor')
AS purchases(customer, product);

-- With duplicates
SELECT customer, COLLECT_LIST(product) AS all_items
FROM purchases GROUP BY customer;
-- Alice → [laptop, mouse, laptop]

-- Without duplicates
SELECT customer, COLLECT_SET(product) AS unique_items
FROM purchases GROUP BY customer;
-- Alice → [laptop, mouse]
```

## 🧠 COLLECT_LIST vs COLLECT_SET

| Feature | `COLLECT_LIST` | `COLLECT_SET` |
|---------|---------------|--------------|
| Duplicates | Preserved | Removed |
| NULLs | Excluded | Excluded |
| Ordering | Non-deterministic | Non-deterministic |

See [List Functions](../../../../function/collection/list.md) for more patterns.
