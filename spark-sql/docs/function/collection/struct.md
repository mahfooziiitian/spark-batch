# :material-cube-outline: Struct Functions

A **struct** (also called a named struct) groups multiple named fields into a single composite
value. Structs are the Spark SQL equivalent of a row or record within a column.

### :material-sitemap: Overview

```mermaid
graph LR
    A[Named Fields] --> B[Struct Functions]
    B --> C[Struct Type]
```

## :material-pin: Creating Structs

### NAMED_STRUCT — Explicit Field Names

```sql
SELECT NAMED_STRUCT('name', 'Alice', 'age', 30, 'city', 'NYC') AS person;
-- Result: {name: Alice, age: 30, city: NYC}
```

### STRUCT — Auto-Named Fields

```sql
SELECT STRUCT('Alice', 30, 'NYC') AS person;
-- Result: {col1: Alice, col2: 30, col3: NYC}
```

> `STRUCT` auto-names fields as `col1`, `col2`, etc. Use `NAMED_STRUCT` for explicit names.

## :material-pin: Accessing Struct Fields

### Dot Notation (Most Common)

```sql
SELECT person.name, person.age
FROM (
  SELECT NAMED_STRUCT('name', 'Alice', 'age', 30) AS person
);
-- Result: Alice, 30
```

### ELEMENT_AT

```sql
SELECT ELEMENT_AT(person, 'name') AS name
FROM (
  SELECT NAMED_STRUCT('name', 'Alice', 'age', 30) AS person
);
-- Result: Alice
```

## :material-magnify: Behavior

1. Structs enforce a fixed schema — each field has a name and type.
2. Fields are accessed by name using dot notation (`col.field`).
3. Structs can be nested: `NAMED_STRUCT('address', NAMED_STRUCT('city', 'NYC'))`.
4. Structs can contain any type: primitives, arrays, maps, or other structs.
5. Two structs are equal if all their fields are equal (field-by-field comparison).

## :material-flask-outline: Practical Examples

### :material-toy-brick: 1. Create and Query a Struct Column

```sql
CREATE OR REPLACE TEMP VIEW employees AS
SELECT * FROM VALUES
  (1, NAMED_STRUCT('first', 'Alice', 'last', 'Smith'), 50000),
  (2, NAMED_STRUCT('first', 'Bob', 'last', 'Jones'), 60000)
AS employees(id, name, salary);

SELECT id, name.first, name.last, salary
FROM employees;
-- (1, Alice, Smith, 50000), (2, Bob, Jones, 60000)
```

### :material-toy-brick: 2. Nested Structs

```sql
SELECT NAMED_STRUCT(
  'name', 'Alice',
  'address', NAMED_STRUCT('city', 'NYC', 'state', 'NY')
) AS person;

-- Access nested field
SELECT person.address.city
FROM (
  SELECT NAMED_STRUCT(
    'name', 'Alice',
    'address', NAMED_STRUCT('city', 'NYC', 'state', 'NY')
  ) AS person
);
-- Result: NYC
```

### :material-toy-brick: 3. Array of Structs

```sql
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (1, ARRAY(
    NAMED_STRUCT('product', 'book', 'qty', 2),
    NAMED_STRUCT('product', 'pen', 'qty', 5)
  ))
AS orders(order_id, items);

-- Access struct fields after EXPLODE
SELECT order_id, item.product, item.qty
FROM orders
LATERAL VIEW EXPLODE(items) AS item;
```

### :material-toy-brick: 4. Build Structs in Aggregations

```sql
SELECT department,
       NAMED_STRUCT(
         'count', COUNT(*),
         'avg_salary', ROUND(AVG(salary), 2),
         'max_salary', MAX(salary)
       ) AS stats
FROM employees
GROUP BY department;
```

### :material-toy-brick: 5. Compare Structs

```sql
SELECT NAMED_STRUCT('a', 1, 'b', 2) = NAMED_STRUCT('a', 1, 'b', 2) AS eq;
-- Result: true

SELECT NAMED_STRUCT('a', 1, 'b', 2) = NAMED_STRUCT('a', 1, 'b', 3) AS eq;
-- Result: false
```

### :material-toy-brick: 6. Use with INLINE to Flatten

```sql
SELECT INLINE(ARRAY(
  NAMED_STRUCT('name', 'Alice', 'score', 95),
  NAMED_STRUCT('name', 'Bob', 'score', 87)
));
-- (Alice, 95), (Bob, 87)
```

## :material-brain: When to Use

| Scenario | Why Structs? |
|----------|-------------|
| Group related fields into a single column | Cleaner schema than multiple columns |
| Return composite values from expressions | `NAMED_STRUCT` in SELECT or CASE |
| Build nested / hierarchical data | Structs within structs, or arrays of structs |
| Aggregate summary statistics | Pack `COUNT`, `AVG`, `MAX` into one column |
| Interop with JSON / Parquet | Struct maps naturally to nested JSON objects |

> **Tip:** Prefer `NAMED_STRUCT` over `STRUCT` in production code — explicit field names
> make queries self-documenting and less error-prone.
