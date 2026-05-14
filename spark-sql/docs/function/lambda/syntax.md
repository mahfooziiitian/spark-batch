# :material-code-parentheses: Lambda Syntax

---

## :material-pin: Basic Forms

### Single parameter

```sql
-- element -> expression
SELECT TRANSFORM(ARRAY(1, 2, 3, 4), x -> x * x) AS squares;
-- Result: [1, 4, 9, 16]
```

### Element + index (two parameters)

The second parameter receives the **zero-based** position of the element.

```sql
SELECT TRANSFORM(
    ARRAY('alpha', 'beta', 'gamma'),
    (val, idx) -> CONCAT(CAST(idx + 1 AS STRING), '. ', val)
) AS numbered;
-- Result: ['1. alpha', '2. beta', '3. gamma']
```

### Key + value (map lambda)

```sql
SELECT MAP_FILTER(
    MAP('a', 1, 'b', 2, 'c', 3),
    (k, v) -> v >= 2
) AS filtered_map;
-- Result: {b -> 2, c -> 3}
```

### Accumulator + element (aggregate merge)

```sql
SELECT AGGREGATE(
    ARRAY(5, 10, 15),
    0,
    (acc, x) -> acc + x
) AS total;
-- Result: 30
```

### Accumulator + finish function

```sql
-- Merge lambda + finish lambda
SELECT AGGREGATE(
    ARRAY(5, 10, 15),
    0,
    (acc, x) -> acc + x,
    acc -> acc * 2        -- post-process: double the sum
) AS doubled_sum;
-- Result: 60
```

### Two-array merge (ZIP_WITH)

```sql
SELECT ZIP_WITH(
    ARRAY(1, 2, 3),
    ARRAY(100, 200, 300),
    (a, b) -> a + b
) AS combined;
-- Result: [101, 202, 303]
```

### Map merge (MAP_ZIP_WITH)

```sql
SELECT MAP_ZIP_WITH(
    MAP('x', 1, 'y', 2),
    MAP('x', 10, 'y', 20),
    (k, v1, v2) -> COALESCE(v1, 0) + COALESCE(v2, 0)
) AS merged;
-- Result: {x -> 11, y -> 22}
```

---

## :material-information: Parameter Rules

| Rule | Detail |
|------|--------|
| Parameter names | Arbitrary — use descriptive names (`price`, `person`, `acc`) |
| Parameter types | Inferred from the array element / map key-value types |
| Parameter count | Must match the HOF's expected lambda signature |
| Scope | Parameters are only visible inside the lambda expression |
| Side effects | None — lambdas are pure expressions |

---

## :material-layers: Nested Lambdas

Lambdas can be nested for arrays-of-arrays or arrays-of-maps.

```sql
-- Array of arrays: double every inner element
SELECT TRANSFORM(
    ARRAY(ARRAY(1, 2), ARRAY(3, 4, 5)),
    inner -> TRANSFORM(inner, x -> x * 2)
) AS doubled;
-- Result: [[2, 4], [6, 8, 10]]

-- Array of maps: filter each inner map
SELECT TRANSFORM(
    ARRAY(
        MAP('a', 1, 'b', 5),
        MAP('a', 3, 'b', 2)
    ),
    m -> MAP_FILTER(m, (k, v) -> v > 2)
) AS filtered_maps;
-- Result: [{b -> 5}, {a -> 3}]
```

---

## :material-function-variant: Using SQL Expressions Inside Lambdas

Any valid SQL expression is allowed in the lambda body:

```sql
-- CASE WHEN
SELECT TRANSFORM(ARRAY(85, 42, 97, 55),
    score -> CASE
        WHEN score >= 90 THEN 'A'
        WHEN score >= 70 THEN 'B'
        WHEN score >= 50 THEN 'C'
        ELSE 'F'
    END
) AS grades;
-- Result: ['B', 'F', 'A', 'C']

-- String functions
SELECT TRANSFORM(ARRAY('  hello ', ' world  '), x -> UPPER(TRIM(x))) AS cleaned;
-- Result: ['HELLO', 'WORLD']

-- Struct field access
SELECT FILTER(
    ARRAY(
        NAMED_STRUCT('name', 'Alice', 'age', 25),
        NAMED_STRUCT('name', 'Bob',   'age', 17)
    ),
    p -> p.age >= 18
) AS adults;
-- Result: [{name: Alice, age: 25}]
```

---

## :material-magnify: Behavior Notes

1. **Lambdas are not standalone** — they only appear as arguments to HOFs; `SELECT x -> x * 2` is invalid.
2. **Type inference** — Spark infers lambda parameter types from the containing array/map; explicit casts are rarely needed.
3. **NULL propagation** — if an element is NULL the lambda receives NULL; guard with `COALESCE` or `IF(x IS NULL, ...)` inside the lambda.
4. **Cannot call UDFs inside lambdas** — only built-in SQL functions are allowed inside lambda expressions.
5. **No pushdown** — HOFs with lambdas are evaluated in Spark memory; they cannot be pushed to Parquet/Delta scan filters.
