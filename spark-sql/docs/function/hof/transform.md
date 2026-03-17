# Transform

The `TRANSFORM()` higher-order function applies a lambda expression to **each element** of an
array and returns a new array of the results — similar to `map()` in functional programming.

## 📌 Syntax

```sql
TRANSFORM(array, element -> expression)

-- With index access
TRANSFORM(array, (element, index) -> expression)
```

### Parameters

| Parameter | Description |
|-----------|-------------|
| `array` | Input array to transform |
| `element` | Variable representing each array element |
| `index` | *(Optional)* Zero-based position of the element |
| `expression` | Logic to apply; its result becomes the new element |

## 🔍 Behavior

1. Iterates over each element and applies the expression.
2. Returns a new array with the same length as the input.
3. The output element type is determined by the expression's return type.
4. Returns `NULL` if the input array is `NULL`.
5. Works on nested types: `array<array<T>>`, `array<struct<...>>`.

## 🧪 Practical Examples

### 🧱 1. Double Each Element

```sql
SELECT TRANSFORM(ARRAY(1, 2, 3), x -> x * 2) AS doubled;
-- Result: [2, 4, 6]
```

### 🧱 2. Add Index to Each Element

```sql
SELECT TRANSFORM(ARRAY(10, 20, 30), (x, i) -> x + i) AS indexed;
-- Result: [10, 21, 32]
```

### 🧱 3. Conditional Transformation

```sql
SELECT TRANSFORM(
  ARRAY(1, 2, 3, 4),
  x -> CASE WHEN x < 3 THEN 0 ELSE x END
) AS conditioned;
-- Result: [0, 0, 3, 4]
```

### 🧱 4. Transform Struct Fields

```sql
SELECT TRANSFORM(
  ARRAY(
    NAMED_STRUCT('name', 'Alice', 'age', 25),
    NAMED_STRUCT('name', 'Bob', 'age', 30)
  ),
  s -> NAMED_STRUCT('name', UPPER(s.name), 'age', s.age + 1)
) AS updated;
-- Result: [{name: ALICE, age: 26}, {name: BOB, age: 31}]
```

### 🧱 5. Nested Array Transformation

```sql
SELECT TRANSFORM(
  ARRAY(ARRAY(1, 2), ARRAY(3, 4)),
  a -> TRANSFORM(a, x -> x * 10)
) AS nested;
-- Result: [[10, 20], [30, 40]]
```

### 🧱 6. Format Elements as Strings

```sql
SELECT TRANSFORM(
  ARRAY(1, 2, 3),
  (x, i) -> CONCAT('item_', CAST(i AS STRING), '=', CAST(x AS STRING))
) AS labels;
-- Result: ['item_0=1', 'item_1=2', 'item_2=3']
```

### 🧱 7. Extract Fields from Array of Structs

```sql
SELECT TRANSFORM(
  ARRAY(
    NAMED_STRUCT('product', 'Widget', 'price', 10.0),
    NAMED_STRUCT('product', 'Gadget', 'price', 25.0)
  ),
  s -> s.product
) AS product_names;
-- Result: ['Widget', 'Gadget']
```

## 🏭 Real-World Applications

### Normalize Sensor Readings

```sql
CREATE OR REPLACE TEMP VIEW sensors AS
SELECT * FROM VALUES
  ('dev01', ARRAY(25.0, 105.0, 98.0, 5.0)),
  ('dev02', ARRAY(45.0, 30.0, 110.0, 0.0))
AS sensors(device_id, readings);

SELECT device_id,
       TRANSFORM(readings, x -> ROUND(x / 100.0, 2)) AS normalized
FROM sensors;
-- dev01 → [0.25, 1.05, 0.98, 0.05]
```

### Tag Array Elements with Position

```sql
SELECT TRANSFORM(
  ARRAY('error', 'warn', 'info'),
  (msg, i) -> CONCAT('[', CAST(i AS STRING), '] ', msg)
) AS tagged_logs;
-- Result: ['[0] error', '[1] warn', '[2] info']
```

## 🧠 When to Use

| Scenario | Why `TRANSFORM`? |
|----------|-----------------|
| Apply a formula to every array element | Avoids explode + collect_list round-trip |
| Convert / cast element types | Transform `array<int>` → `array<string>` in place |
| Extract a single field from structs | Produces a flat array from `array<struct>` |
| Enrich elements with positional info | Two-param form gives zero-based index |
| Nested array operations | Recursive `TRANSFORM` for multi-level arrays |
| Pipeline with other HOFs | `TRANSFORM(FILTER(...), ...)` for filter-then-map |

> **Tip:** `TRANSFORM` + `FILTER` is the most common HOF combination — filter unwanted
> elements first, then transform the survivors in a single expression chain.
