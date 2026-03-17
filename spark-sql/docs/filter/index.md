# Filtering Data in Spark SQL

Filtering reduces the number of rows processed and returned, which improves
both performance and query correctness. Spark SQL supports multiple filtering
styles depending on where and when you want the filter to apply.

## 📌 Core Filtering Clauses

| Clause | Applies To | When It Runs | Typical Use |
|--------|------------|--------------|-------------|
| `WHERE` | Rows | Before aggregation | Row-level filters |
| `HAVING` | Groups | After aggregation | Filter grouped results |
| `FILTER` | Aggregates | During aggregation | Conditional aggregates |
| `CASE WHEN` | Expressions | During evaluation | Build flags or conditional logic |
| `LATERAL VIEW` | Exploded rows | During row expansion | Filter inside arrays/maps |

## 🔍 Behavior Notes

1. **Predicate pushdown** — Data sources (Parquet, ORC, Delta) can apply filters
   during scan to avoid reading unnecessary data files.
2. **Partition pruning** — Filters on partition columns skip entire partitions.
3. **NULL logic** — Comparisons with NULL return NULL (not TRUE). Use `IS NULL`,
   `IS NOT NULL`, or the null-safe equality operator `<=>`.
4. **UDFs block pushdown** — User-defined functions often prevent pushdown and
   can force full scans.
5. **Order of operations** — `WHERE` runs before `GROUP BY`, `HAVING` runs after
   aggregation, and `FILTER` runs inside an individual aggregate.

## 🧪 Quick Examples

### Basic WHERE Filter

```sql
SELECT * FROM orders
WHERE order_status = 'SHIPPED'
  AND order_date >= '2024-01-01';
```

### HAVING Filter (After Aggregation)

```sql
SELECT customer_id, SUM(amount) AS total_spend
FROM orders
GROUP BY customer_id
HAVING SUM(amount) > 1000;
```

### FILTER Clause (Per-Aggregate)

```sql
SELECT
  COUNT(*) AS total_orders,
  COUNT(*) FILTER (WHERE status = 'shipped') AS shipped_orders
FROM orders;
```

### Subquery Filter

```sql
SELECT * FROM orders
WHERE customer_id IN (
  SELECT id FROM customers WHERE country = 'US'
);
```

### Complex Types

```sql
SELECT * FROM events
WHERE array_contains(tags, 'priority');
```

---

## 🧠 When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Filter raw rows | `WHERE` |
| Filter aggregated results | `HAVING` |
| Multiple conditional metrics | `FILTER` on aggregates |
| Filter nested arrays/maps | `LATERAL VIEW` + `explode` |
| Handle NULL equality | `<=>` or `IS NULL` |

---

### Related Guides

- [Aggregate FILTER](agg_filter/index.md)
- [CASE WHEN Filters](case_when.md)
- [NULL Handling](null_filter.md)
- [Predicate Pushdown](pp.md)
- [Subquery Filters](sub-query.md)
- [Complex Type Filters](complex/array.md)
