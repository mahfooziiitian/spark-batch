# Left anti join

A left anti join is a type of join operation that returns `only the rows from the left` table that do not have corresponding matches in the right table.

It can be very useful for identifying records in one dataset that do not exist in another.

## 🔁 Anti Join Flow in Spark

1. Spark evaluates the join condition.
2. It identifies rows in the left DataFrame for which no match is found in the right DataFrame.
3. Returns only those unmatched rows.

## Flow

```{mermaid}
flowchart TD
    A["Left Table (L)"] --> J{Does L.id match R.id?}
    R["Right Table (R)"] --> J

    J -- Yes --> D1[Discard row from L]
    J -- No --> D2[Include row from L in result]

    D2 --> O[Final Anti Join Output]
```

## Under the Hood (in Spark)

Condition               | Spark Strategy
------------------------|--------------------------
Join keys + equi-join   | Broadcast or Hash join
No join key or non-equi | Nested Loop Join
Large input             | Sort-Merge Join if needed

## Create table employees

```sql
drop table if exists employee;
drop table if exists departments;
CREATE TABLE employee (id INT, name VARCHAR(50), age INT, department VARCHAR(50));

INSERT INTO employee (id, name, age, department)
  VALUES (1, 'John Doe', 30, 'IT');

INSERT INTO employee (id, name, age, department)
  VALUES (2, 'Jane Smith', 25, 'HR'), (3, 'Michael Johnson', 35, 'Finance');

INSERT INTO employee (id, name, age, department)
  VALUES (4, 'Mahfooz Doe', 30, 'HR');

CREATE TABLE department (department_id INT, department_name VARCHAR(50));

INSERT INTO department (department_id, department_name)
  VALUES (1, 'IT');

INSERT INTO department (department_id, department_name)
  VALUES (2, 'HR'), (3, 'Finance');

INSERT INTO department (department_id, department_name)
  VALUES (4, 'Admin');
```

## Joining

All employees without a matching department.

```sql
select
  *
from
  employee
    LEFT ANTI JOIN department
      on department_name = department;
```

### Equivalent SQL

```sql
SELECT *
FROM orders o
WHERE o.customer_id NOT IN (
  SELECT id FROM customers
)
```

## Use Cases
Use Case                        | Why Anti Join?
--------------------------------|---------------------------------
Filter unmatched rows           | "Find items not in list"
Implement NOT IN / NOT EXISTS   | More scalable than subqueries
Data validation / gap detection | Track orphan rows, missing links

## Optimization Tips

Use broadcast hint if right side is small:

```sql
SELECT /*+ BROADCAST(c) */ ...
```

1. Avoid NOT IN if possible — use anti join.
2. Ensure join keys are not null to avoid unexpected filtering.
