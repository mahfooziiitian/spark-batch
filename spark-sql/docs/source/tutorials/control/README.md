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
