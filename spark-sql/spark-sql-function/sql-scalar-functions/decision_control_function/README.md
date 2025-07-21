# Decision control function

In Spark SQL, control flow functions are used to apply conditional logic — similar to IF, CASE, COALESCE, etc. in traditional SQL. These functions are essential for data transformation, cleaning, and business logic implementation.

## 🔁 1. CASE WHEN

Conditional branching logic — similar to IF...ELSE IF...ELSE.

### Syntax

```sql
CASE
  WHEN condition1 THEN result1
  WHEN condition2 THEN result2
  ...
  ELSE default_result
END
```

### Example

```sql
CREATE OR REPLACE TEMP VIEW employees AS
SELECT * FROM VALUES
  (1, 'Alice', 55000),
  (2, 'Bob', NULL),
  (3, 'Charlie', 25000),
  (4, 'Diana', 75000),
  (5, 'Eve', 0)
AS emp(id, name, salary);
SELECT
  id,
  name,
  salary,
  CASE
    WHEN salary IS NULL THEN 'Unknown'
    WHEN salary >= 50000 THEN 'High'
    WHEN salary BETWEEN 20000 AND 49999 THEN 'Medium'
    ELSE 'Low'
  END AS salary_band
FROM employees;
```

## if

Simplified IF-THEN-ELSE expression (like ternary operator).

```sql
IF(condition, true_value, false_value)
```

If condition evaluates to true, then returns true_value; otherwise returns false_value.

```sql
SELECT if(1 < 2, 'a', 'b');
CREATE OR REPLACE TEMP VIEW employees AS
SELECT * FROM VALUES
  (1, 'Alice', 55000),
  (2, 'Bob', NULL),
  (3, 'Charlie', 25000),
  (4, 'Diana', 75000),
  (5, 'Eve', 0)
AS emp(id, name, salary);
SELECT
  id,
  name,
  salary,
  IF(salary >= 50000, 'Yes', 'No') AS is_high_earner
FROM employees;
```

## 🔄 3. COALESCE(expr1, expr2, ..., exprN)

Returns the first non-null expression.

```sql
CREATE OR REPLACE TEMP VIEW employees AS
SELECT * FROM VALUES
  (1, 'Alice', 55000),
  (2, 'Bob', NULL),
  (3, 'Charlie', 25000),
  (4, 'Diana', 75000),
  (5, 'Eve', 0)
AS emp(id, name, salary);
SELECT
  id,
  name,
  COALESCE(salary, 30000) AS salary_fallback
FROM employees;
```

## ⚠️ 4. NULLIF(expr1, expr2)

Returns NULL if expr1 = expr2, otherwise returns expr1.

### Example NULLIF

```sql
CREATE OR REPLACE TEMP VIEW employees AS
SELECT * FROM VALUES
  (1, 'Alice', 55000),
  (2, 'Bob', NULL),
  (3, 'Charlie', 25000),
  (4, 'Diana', 75000),
  (5, 'Eve', 0)
AS emp(id, name, salary);
SELECT
  id,
  name,
  NULLIF(salary, 0) AS salary_or_null
FROM employees;
```

## 🪙 5. NVL(expr1, expr2)

(Same as COALESCE(expr1, expr2))

Returns expr2 if expr1 is null.

### Example NVL

```sql
CREATE OR REPLACE TEMP VIEW employees AS
SELECT * FROM VALUES
  (1, 'Alice', 55000),
  (2, 'Bob', NULL),
  (3, 'Charlie', 25000),
  (4, 'Diana', 75000),
  (5, 'Eve', 0)
AS emp(id, name, salary);
SELECT
  id,
  name,
  NVL(salary, 40000) AS safe_salary
FROM employees;
```

### 🔄 6. CASE with Boolean Flags

```sql
SELECT 
  user_id,
  CASE active
    WHEN true THEN 'Active'
    WHEN false THEN 'Inactive'
  END AS status
FROM users;
```

## ✅ Summary Table

Function |Purpose
----|---
CASE WHEN |Multi-condition branching
IF |Simple condition: IF(cond, val1, val2)
COALESCE| First non-null expression
NULLIF |NULL if both expressions are equal
NVL |Null-safe fallback for a single value
