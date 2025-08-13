# Transform

The `TRANSFORM()` function in Databricks SQL is a higher-order function (HOF) used to apply a `lambda function` to `each element` of an `array`.

It's similar to `map()` in other languages.

## Syntax

```sql
TRANSFORM(array, element -> expression)

TRANSFORM(array, (element, index) -> expression)
```

## What It Does

1. Iterates over each element in the array.
2. Applies the provided expression to each element.
3. Returns a new array with the transformed values.

## Parameters

Parameter |Description
---|---
array |The input array to be transformed
element |The variable representing each array element
index (opt)| The zero-based index of the element
expression |The logic to apply to each element

## Examples

### Doubling Elements in an Array

```sql
SELECT transform(array(1, 2, 3), x -> x * 2) AS doubled_array;
```

### Adding an Index to Each Element

```sql
SELECT transform(array(10, 20, 30), (x, i) -> x + i) AS indexed_array;
```

### Applying a Condition to Elements

```sql
SELECT transform(
  array(1, 2, 3, 4),
  x -> CASE WHEN x < 3 THEN 0 ELSE x END
) AS conditioned_array;
```

### Using a Struct Inside an Array

```sql
SELECT transform(
  array(
    named_struct('name', 'Alice', 'age', 25),
    named_struct('name', 'Bob', 'age', 30)
  ),
  s -> named_struct('name', s.name, 'age', s.age + 1)
) AS transformed_structs;
```

### Nested Arrays Transformation

```sql
SELECT transform(
  array(array(1, 2), array(3, 4)),
  a -> transform(a, x -> x * 2)
) AS nested_transformed;
```

## Use Case Scenarios

1. Transform values in an array-type column.
2. Normalize or conditionally change values.
3. Apply complex logic to nested arrays or structs.
4. Flatten, restructure, or enrich semi-structured data.

## Tips

1. Combine with other HOFs like `filter`, `aggregate`, or `exists` for advanced operations.
2. Works on nested arrays (`array<array<int>>`) and `array<struct<...>>`.
3. Often used in JSON or semi-structured data pipelines.
