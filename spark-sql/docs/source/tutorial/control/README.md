# Control Structure

`control structures` typically refer to conditional logic and flow control functions you can use inside SQL queries, views, and expressions.

While Databricks SQL doesn’t have procedural loops like traditional programming languages (unless you move to Databricks notebooks with PySpark or SQL procedural extensions), it does support CASE, IF, and related expressions to control the flow of logic.

## CASE Expression

Used for conditional branching inside SELECT, WHERE, GROUP BY, etc.

### Syntax

```sql
CASE 
    WHEN condition1 THEN result1
    WHEN condition2 THEN result2
    ...
    ELSE default_result
END
```

```sql
CREATE TABLE sales_table (
    name STRING,
    sales INT
);

INSERT INTO sales_table VALUES
('Alice', 120000),
('Bob', 75000),
('Charlie', 40000),
('David', 25000),
('Eva', 90000);

SELECT 
    name,
    sales,
    CASE 
        WHEN sales >= 100000 THEN 'High'
        WHEN sales >= 50000 THEN 'Medium'
        ELSE 'Low'
    END AS sales_category
FROM sales_table;
```

## IF Function

A shorthand for simple conditional checks.

### Syntax IF

```sql
IF(condition, true_value, false_value)
```

### Example IF

```sql
CREATE TABLE people (
    name STRING,
    age INT
);

INSERT INTO people VALUES
('Alice', 25),
('Bob', 17),
('Charlie', 34),
('David', 15),
('Eva', 22);

SELECT 
    name,
    IF(age >= 18, 'Adult', 'Minor') AS category
FROM people;
```

## NULLIF

```sql
NULLIF(expr1, expr2)
```

Returns NULL if both are equal.

## Control Functions for Multi-Condition Logic

1. IFF() → Equivalent to IF().
2. CASE WHEN for multiple conditions.
3. NVL() → Alias of COALESCE for two arguments.

## Coalesce

```sql
COALESCE(expr1, expr2, ...)
```

Returns first non-null.

```sql
CREATE TABLE users (
    name STRING,
    email STRING
);

INSERT INTO users VALUES
('Alice', 'alice@mail.com'),
('Bob', NULL),
('Charlie', 'charlie@mail.com'),
('David', NULL),
('Eva', 'eva@mail.com');

SELECT 
    name,
    COALESCE(email, 'no_email@example.com') AS safe_email
FROM users;
```
