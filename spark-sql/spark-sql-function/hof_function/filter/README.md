# 🧠 FILTER() – Higher-Order Function in Databricks SQL

## ✅ What It Does

The FILTER() function returns a new array containing only the elements that match a condition.

## 📌 Syntax

```sql
FILTER(array, element -> condition)
```

With Index:

```sql
FILTER(array, (element, index) -> condition)
```

## 🔧 Parameters

Parameter| Description
---|---
array |Input array
element |Variable representing each element
index |(Optional) Element index (0-based)
condition| Boolean expression to filter elements

## Filtering Even Numbers

```sql

SELECT filter(array(1, 2, 3, 4, 5), x -> x % 2 = 0) AS even_numbers;
```

## Filtering Based on a Condition

```sql

SELECT filter(array(1, 2, 3, 4, 5), x -> x > 2) AS greater_than_two;
```

## Filtering Null Values

```sql

SELECT filter(array(1, NULL, 3, NULL, 5), x -> x IS NOT NULL) AS no_nulls;
```

## Filtering Nested Arrays

```sql

SELECT filter(array(array(1, 2), array(), array(3, 4)), a -> size(a) > 0) AS non_empty_arrays;
```

## Filtering Based on String Length

```sql

SELECT filter(array('apple', 'cat', 'banana', 'dog'), x -> length(x) > 3) AS long_strings;
```

## Filtering Struct Elements

```sql

SELECT filter(
    array(named_struct('name', 'Alice', 'age', 25), named_struct('name', 'Bob', 'age', 30)),
    s -> s.age > 25
) AS filtered_structs;
```

## Un named struct

```sql

SELECT FILTER(
        ARRAY(STRUCT('Alice', 25), STRUCT('Bob', 30)),
        s -> s.col2 > 25
) AS filtered_structs;
```

## 🔍 Behavior

1. Returns a new array of elements where the condition is true.
2. Preserves the order of the original array.
3. Returns an empty array if no elements match.
4. Returns NULL if input array is NULL.
