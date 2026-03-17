# Aggregate

The `AGGREGATE()` higher-order function reduces the elements of an array into a single scalar
value using custom lambda logic — similar to `reduce()` / `fold()` in functional programming.

## 📌 Syntax

```sql
AGGREGATE(array, initial_value, merge_function [, finish_function])
```

### Parameters

| Parameter | Description |
|-----------|-------------|
| `array` | The array to reduce |
| `initial_value` | Starting accumulator value (determines output type) |
| `merge_function` | `(acc, element) -> new_acc` — combines accumulator with each element |
| `finish_function` | *(Optional)* `acc -> result` — transforms the final accumulator |

## 🔍 Behavior

1. Initializes the accumulator to `initial_value`.
2. Iterates through each element of the array, applying `merge_function` to update the accumulator.
3. If `finish_function` is provided, applies it to the final accumulator before returning.
4. Returns `NULL` if the input array is `NULL`.
5. The type of `initial_value` determines the accumulator and return type.

## 🧪 Practical Examples

### 🧱 1. Sum All Elements

```sql
SELECT AGGREGATE(ARRAY(1, 2, 3, 4, 5), 0, (acc, x) -> acc + x) AS total;
-- Result: 15
```

### 🧱 2. Concatenate Strings

```sql
SELECT AGGREGATE(
  ARRAY('spark', 'sql', 'hof'), '',
  (acc, x) -> CASE WHEN acc = '' THEN x ELSE acc || ', ' || x END
) AS joined;
-- Result: 'spark, sql, hof'
```

### 🧱 3. Find the Maximum Value

```sql
SELECT AGGREGATE(
  ARRAY(10, 42, 7, 35), 0,
  (acc, x) -> CASE WHEN x > acc THEN x ELSE acc END
) AS max_val;
-- Result: 42
```

### 🧱 4. Count Non-Null Elements

```sql
SELECT AGGREGATE(
  ARRAY(1, NULL, 3, NULL, 5), 0,
  (acc, x) -> acc + CASE WHEN x IS NOT NULL THEN 1 ELSE 0 END
) AS non_null_count;
-- Result: 3
```

### 🧱 5. Sum with a Finish Function (Average)

```sql
SELECT AGGREGATE(
  ARRAY(10, 20, 30, 40),
  NAMED_STRUCT('total', 0D, 'cnt', 0),
  (acc, x) -> NAMED_STRUCT('total', acc.total + x, 'cnt', acc.cnt + 1),
  acc -> acc.total / acc.cnt
) AS average;
-- Result: 25.0
```

### 🧱 6. Sum Nested Arrays

```sql
SELECT AGGREGATE(
  ARRAY(ARRAY(1, 2), ARRAY(3, 4), ARRAY(5)),
  0,
  (acc, a) -> acc + AGGREGATE(a, 0, (inner_acc, x) -> inner_acc + x)
) AS nested_sum;
-- Result: 15
```

### 🧱 7. Aggregate Struct Fields

```sql
SELECT AGGREGATE(
  ARRAY(
    NAMED_STRUCT('name', 'Alice', 'score', 90),
    NAMED_STRUCT('name', 'Bob', 'score', 85)
  ),
  0,
  (acc, s) -> acc + s.score
) AS total_score;
-- Result: 175
```

## 🧠 When to Use

| Scenario | Why `AGGREGATE`? |
|----------|-----------------|
| Custom sum / product / running total | Built-in `SUM` doesn't work on array elements directly |
| Concatenating array elements | No built-in `ARRAY_JOIN` in all Spark versions |
| Computing averages from arrays | Combine struct accumulator with finish function |
| Nested array reduction | Recursive `AGGREGATE` calls handle nested structures |
| Conditional counting / filtering | Combine `CASE` logic inside the merge lambda |

> **Tip:** The `finish_function` is powerful for post-processing — use it for averages, formatting,
> or any transformation that only makes sense after the full reduction is complete.
