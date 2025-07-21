# Aggregate filter

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('Alice', 'North', 'A', 100),
  ('Bob', 'North', 'B', 150),
  ('Alice', 'South', 'A', 200),
  ('Bob', 'South', 'B', 300),
  ('Charlie', 'North', 'C', 400),
  ('Alice', 'North', 'B', 250),
  ('Charlie', 'South', 'A', 350)
AS sales(name, region, product, amount);
```

## 🧠 Why Use Aggregate Filters?

Advantage| Example
---|---
Efficient multi-metric reports| SUM(...) FILTER (WHERE ...)
Avoids multiple scans or joins| All in one query
Clean, expressive syntax |No messy CASE WHEN blocks

## 🔍 Key Differences

Feature |FILTER (WHERE ...) | HAVING
---|---|--
Purpose |Filters within an aggregation| Filters entire grouped results
Applies to| Specific aggregate function| The row after aggregation
Filter Timing| During aggregation| After GROUP BY aggregation is done
Syntax Position| Inside the SELECT clause| After GROUP BY clause
Granularity |Per aggregate |Per group

## 🧠 When to Use

Use Case| Use
---|---
You want multiple conditional aggregates in one query| ✅ FILTER (WHERE...)
You want to exclude groups after aggregation (like post-filtering) |✅ HAVING
You want to do both — compute filtered metrics and also restrict result set |✅ Use both
