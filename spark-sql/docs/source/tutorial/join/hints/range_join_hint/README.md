# Range Join Hint in Spark SQL

Leverage **range join hints** in Spark SQL to optimize join operations involving range conditions. This feature can significantly improve query performance for range-based joins.

---

## 🚀 Enabling Range Join with Hints

Use the `RANGE_JOIN` hint to instruct the optimizer to use a range join strategy. Specify the target table and an appropriate bin size for partitioning.

```sql
-- Example 1: Range join between 'points' and 'ranges'
SELECT /*+ RANGE_JOIN(points, 10) */ *
FROM points
JOIN ranges
    ON points.p >= ranges.start AND points.p < ranges.end;
```

```sql
-- Example 2: Range join with subqueries and multiple tables
SELECT /*+ RANGE_JOIN(r1, 0.1) */ *
FROM (SELECT * FROM ranges WHERE amount < 100) r1, ranges r2
WHERE r1.start < r2.start + 100
    AND r2.start < r1.start + 100;
```

```sql
-- Example 3: Range join with multiple join conditions
SELECT /*+ RANGE_JOIN(c, 500) */ *
FROM a
JOIN b ON a.b_key = b.id
JOIN c ON a.ts BETWEEN c.start_time AND c.end_time;
```

---

## ⚙️ Configuration

Set the bin size for range joins to control partitioning granularity:

```sql
SET spark.databricks.optimizer.rangeJoin.binSize = 5;
```

---

## 📊 Analyzing Range Distribution

Estimate the distribution of range widths using `APPROX_PERCENTILE`:

```sql
SELECT APPROX_PERCENTILE(CAST(end - start AS DOUBLE), ARRAY(0.5, 0.9, 0.99, 0.999, 0.9999))
FROM ranges;
```

---

**References:**

- [Databricks Documentation: Range Join Optimization](https://docs.databricks.com/en/sql/language-manual/hints.html#range_join)
