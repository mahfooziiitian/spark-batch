# :material-table-pivot: unpivot

UNPIVOT is a data transformation that turns columns into rows.

## Simple Analogy

If PIVOT makes a table wider (by turning row values into columns),
then UNPIVOT makes it taller (by turning columns into rows).

## Why Use UNPIVOT?

1. You want to normalize wide data into a more usable format.
2. Helpful for aggregations, filtering, and grouping by category.
3. Many analytics and ML tools prefer long/tidy format.

## Real-World Use Cases
Use Case       | Description
---------------|---------------------------------------------------------------
Survey Results | Each question is a column → UNPIVOT to analyze answers
Sensor Data    | Temperature, Humidity, etc. → UNPIVOT to sensor_type and value
Finance        | Turn monthly columns into date-value pairs

## SQL

```sql
SELECT name, subject, score
FROM (
  SELECT *
  FROM students
) tmp
LATERAL VIEW STACK(3,
    'math', math,
    'science', science,
    'history', history
) AS subject, score;
```
