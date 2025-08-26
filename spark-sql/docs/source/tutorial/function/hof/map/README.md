# Introduction

In Spark SQL, there are higher-order functions (HOFs) like map_filter, map_keys, map_values, map_concat, etc., that allow functional-style operations on complex types (MAP, ARRAY).

## map_filter

Filters entries in a map.

```sql
map_filter(map<K,V>, function<K,V,Boolean>) → map<K,V>
```

1. `Input`: a MAP<K, V>
2. `Function`: a lambda function (key, value) -> boolean
3. `Output`: a new map that only contains entries for which the lambda returns true.

```sql
SELECT map_filter(map(1,'a',2,'bb',3,'ccc'), (k,v) -> length(v) > 1);
```

## map_with_zip

```sql
map_zip_with(map<K,V1>, map<K,V2>, function<K,V1,V2,V3>) → map<K,V3>
```

1. Takes two arrays (array1, array2).
2. Zips them together by index.
3. Applies a lambda to produce (key, value) pairs.
4. Returns a MAP<key, value>

```sql
SELECT map_zip_with(map(1,10,2,20), map(1,1,2,2), (k,v1,v2) -> v1+v2);
```

### Turn two arrays into a key-value map

```sql

WITH dataset AS (
    SELECT array('env','version','owner') AS keys,
           array('prod','1.2','teamA') AS values
)
SELECT map_with_zip(keys, values, (k,v) -> (k, v)) AS attributes
FROM dataset;
```

## transform_map

```sql
transform_keys(map<K,V>, function<K,V,K2>) → map<K2,V>
```

```sql
SELECT transform_keys(map(1, 'a', 2, 'b'), (k, v) -> k * 10) AS new_map;
```

## transform_values

```sql
transform_values(map<K,V>, function<K,V,V2>) → map<K,V2>
```

```sql
SELECT transform_values(map(1, 'a', 2, 'b'), (k, v) -> upper(v)) AS new_map;
```
