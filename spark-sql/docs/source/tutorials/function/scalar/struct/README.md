# Struct

In Databricks SQL, a `struct` scalar function is any built-in or user-defined function that works with `STRUCT` data types (also called structs or `named structs`).

## named_struct

```sql
named_struct(name1, val1, name2, val2, ...)
```

It creates a struct with the given field names and values.

```sql
SELECT named_struct("a", 1, "b", 2, "c", 3);
```

## struct

```sql
SELECT struct("a", 1, "b", 2, "c", 3);
```

## Dot notation (most common)

```sql
SELECT person.name, person.age
FROM (
    SELECT named_struct('name', 'Alice', 'age', 30) AS person
);
```

## Using element_at

```sql
SELECT element_at(person, 'name') AS name
FROM (
    SELECT named_struct('name', 'Alice', 'age', 30) AS person
);
```
