# Introduction

In Spark SQL, there are higher-order functions (HOFs) like map_filter, map_keys, map_values, map_concat, etc., that allow functional-style operations on complex types (MAP, ARRAY).

## map_filter

Filters entries in a map.

```sql
map_filter(map<K, V>, function<K, V, boolean>)
```

1. `Input`: a MAP<K, V>
2. `Function`: a lambda function (key, value) -> boolean
3. `Output`: a new map that only contains entries for which the lambda returns true.

## map_with_zip

```sql
map_zip_with(map<K, V1>, map<K, V2>, function<K, V1, V2, R>)
```

1. Takes two arrays (array1, array2).
2. Zips them together by index.
3. Applies a lambda to produce (key, value) pairs.
4. Returns a MAP<key, value>

### Turn two arrays into a key-value map

```sql

WITH dataset AS (
    SELECT array('env','version','owner') AS keys,
           array('prod','1.2','teamA') AS values
)
SELECT map_with_zip(keys, values, (k,v) -> (k, v)) AS attributes
FROM dataset;
```
