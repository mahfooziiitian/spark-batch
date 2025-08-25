# aggregate

The `aggregate` higher-order function in Spark SQL enables you to reduce the elements of an `array` into a single value using custom logic. This is especially useful for complex aggregations that go beyond built-in functions.

## Syntax

```sql
aggregate(array, initialValue, mergeFunction, finishFunction)
```

- **array**: The array to aggregate.
- **initialValue**: The starting value for aggregation.
- **mergeFunction**: Lambda function to combine the accumulator with each element.
- **finishFunction** (optional): Lambda function to transform the final accumulator value.

---

## Examples

### 1. Summing Elements in an Array

```sql
SELECT aggregate(array(1, 2, 3, 4, 5), 0, (acc, x) -> acc + x) AS sum;
```

**Result:** `15`

---

### 2. Concatenating Strings

```sql
SELECT aggregate(array('a', 'b', 'c'), '', (acc, x) -> 
    CASE WHEN acc = '' THEN x ELSE acc || ',' || x END
) AS concatenated;
```

**Result:** `'a,b,c'`

---

### 3. Finding the Maximum Value

```sql
SELECT aggregate(array(1, 2, 3, 4, 5), 0, (acc, x) -> 
    CASE WHEN x > acc THEN x ELSE acc END
) AS max_value;
```

**Result:** `5`

---

### 4. Counting Non-Null Elements

```sql
SELECT aggregate(array(1, NULL, 3, NULL, 5), 0, (acc, x) -> 
    acc + CASE WHEN x IS NOT NULL THEN 1 ELSE 0 END
) AS non_null_count;
```

**Result:** `3`

---

### 5. Summing Elements of Nested Arrays

```sql
SELECT aggregate(
    array(array(1, 2), array(3, 4), array(5)),
    0,
    (acc, a) -> acc + aggregate(a, 0, (inner_acc, x) -> inner_acc + x)
) AS nested_sum;
```

**Result:** `15`

---

### 6. Aggregating Struct Elements

```sql
SELECT aggregate(
    array(
        named_struct('name', 'Alice', 'age', 25),
        named_struct('name', 'Bob', 'age', 30)
    ),
    0,
    (acc, s) -> acc + s.age
) AS total_age;
```

**Result:** `55`

---

## References

- [Spark SQL Documentation: aggregate](https://spark.apache.org/docs/latest/api/sql/index.html#aggregate)
- [Higher-Order Functions in Spark SQL](https://spark.apache.org/docs/latest/sql-ref-functions.html#higher-order-functions)

---

> **Tip:** Use `aggregate` for custom reductions when built-in functions are not sufficient.
