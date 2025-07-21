# Execution Order in SQL/DF Operations (Operator-wise)

Here’s the logical operator execution order, similar to SQL:

```sql
SELECT columns
FROM table
WHERE filter_condition
GROUP BY columns
HAVING aggregate_condition
ORDER BY columns
LIMIT N
```

The Spark logical execution order is:

1. FROM / JOIN
2. WHERE
3. GROUP BY
4. HAVING
5. SELECT
6. ORDER BY
7. LIMIT

## Examples

```sql
SELECT country, avg(salary) as avg_salary
FROM table
WHERE age > 30
GROUP BY country
HAVING avg(salary) > 50000
ORDER BY avg(salary) DESC
LIMIT 10
```

Execution Order:

1. Read from Parquet (with filter pushdown if possible).
2. Filter age > 30
3. GroupBy country
4. Aggregate avg(salary)
5. Filter avg_salary > 50000 (HAVING)
6. Order By avg_salary DESC
7. Limit to 10 rows
8. show the data
