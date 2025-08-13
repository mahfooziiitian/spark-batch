# Common Table Expressions (CTEs)

In Spark SQL, a CTE (Common Table Expression) lets you define a temporary result set that can be referenced in a SELECT, INSERT, UPDATE, or DELETE statement.

It improves query clarity, modularity, and reusability—just like in standard SQL.  

## ✅ Syntax of CTE in Spark SQL

```sql
WITH cte_name AS (
    SELECT ...
)
SELECT * FROM cte_name WHERE ...;
```

You can also define multiple CTEs.

```sql
WITH cte1 AS (
    SELECT ...
),
cte2 AS (
    SELECT ...
)
SELECT * FROM cte1 JOIN cte2 ON ...;
```

## 🟢 Example

### Single CTE

```sql
CREATE OR REPLACE TEMP VIEW employees AS
SELECT * FROM VALUES
  (1, 'Alice', 120000),
  (2, 'Bob', 95000),
  (3, 'Charlie', 130000),
  (101, 'David', 110000),
  (4, 'Eve', 102000),
  (5, 'Frank', 88000)
AS employees(id, name, salary);
WITH high_salary_employees AS (
    SELECT id, name, salary
    FROM employees
    WHERE salary > 100000
)
SELECT name FROM high_salary_employees WHERE id < 100;
```

### Multiple CTEs

```sql
CREATE OR REPLACE TEMP VIEW employees AS
SELECT * FROM VALUES
  (1, 'Alice',    300000,  10),
  (2, 'Bob',      250000,  10),
  (3, 'Charlie',  300000,  10),
  (4, 'David',    220000,  10),
  (5, 'Eve',      150000,  20),
  (6, 'Frank',    140000,  20),
  (7, 'Grace',    130000,  30)
AS employees(id, name, salary, department_id);
CREATE OR REPLACE TEMP VIEW departments AS
SELECT * FROM VALUES
  (10, 'Engineering'),
  (20, 'Sales'),
  (30, 'Marketing')
AS departments(department_id, department_name);

WITH dept_total AS (
    SELECT department_id, SUM(salary) AS total_salary
    FROM employees
    GROUP BY department_id
),
high_dept AS (
    SELECT department_id
    FROM dept_total
    WHERE total_salary > 1000000
)
SELECT e.name, e.department_id
FROM employees e
JOIN high_dept h ON e.department_id = h.department_id;
```
