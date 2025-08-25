# Map

In Spark SQL, the `map()` function is used to create map literals (i.e. dictionaries with key-value pairs).

You can use it to define static maps or construct maps from column values.

## Syntax of map() in Spark SQL

```sql
map(key1, value1, key2, value2, ..., keyN, valueN)
```

1. It requires an even number of arguments.
2. Keys and values can be column names, expressions, or literals.

## Examples

### 1: Static Map

```sql
SELECT map('a', 1, 'b', 2, 'c', 3) AS my_map
```
