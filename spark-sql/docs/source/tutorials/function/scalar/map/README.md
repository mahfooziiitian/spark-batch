# Map

## element_at

```sql
element_at(map, key)
```

```sql

SELECT element_at(map(1, 'a', 2, 'b'), 2);
```

## map

```sql
map(key0, value0, key1, value1, ...)
```

Creates a map with the given key/value pairs.

```sql
SELECT map(1.0, '2', 3.0, '4');
```

## map_concat

```sql
map_concat(map, ...)
```

Returns the union of all the given maps

```sql
SELECT map_concat(map(1, 'a', 2, 'b'), map(3, 'c'));
```

## map_contains_key

```sql
map_contains_key(map, key)
```

Returns true if the map contains the key.

```sql
SELECT map_contains_key(map(1, 'a', 2, 'b'), 1);
SELECT map_contains_key(map(1, 'a', 2, 'b'), 3);
```

## map_entries

Returns an unordered array of all entries in the given map.

### Syntax map_entries

```sql
map_entries(map)
```

### Example map_entries

```sql
SELECT map_entries(map(1, 'a', 2, 'b'));
```

## map_from_arrays

1. Creates a map with a pair of the given key/value arrays.
2. All elements in keys should not be null

```sql
map_from_arrays(keys, values)
```

### Example map_from_arrays

```sql
SELECT map_from_arrays(array(1.0, 3.0), array('2', '4'));
```

## map_from_entries

Returns a map created from the given array of `entries`.

### Syntax map_from_entries

```sql
map_from_entries(arrayOfEntries)
```

### Example map_from_entries

```sql
SELECT map_from_entries(array(struct(1, 'a'), struct(2, 'b')));
```

## map_keys

Returns an unordered array containing the keys of the map.

### Syntax map_keys

```sql
map_keys(map)
```

### Example map_keys

```sql
SELECT map_keys(map(1, 'a', 2, 'b'));
```

## map_values

Returns an unordered array containing the values of the map.

```sql
map_values(map)
```

```sql
SELECT map_values(map(1, 'a', 2, 'b'));
```

## map_zip_with

map_zip_with(m1, m2, (k, v1, v2) -> expr) returns a new map whose keys are the union of keys from m1 and m2. For each key:

1. v1 is the value from m1 (or NULL if the key is absent in m1)
2. v2 is the value from m2 (or NULL if the key is absent in m2)
3. your lambda produces the output value for that key

4. Merges two given maps into a single map by `applying function` to the pair of values with the `same key`.
5. For keys only presented in one map, NULL will be passed as the value for the missing key.
6. If an input map contains duplicated keys, only the first entry of the duplicated key is passed into the lambda function.

### Syntax map_zip_with

```sql
map_zip_with(m1, m2, (k, v1, v2) -> <expression using k, v1, v2>)
```

### Example map_zip_with

```sql
SELECT map_zip_with(map(1, 'a', 2, 'b'), map(1, 'x', 2, 'y'), (k, v1, v2) -> concat(v1, v2));
SELECT map_zip_with(map('a', 1, 'b', 2), map('b', 3, 'c', 4), (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0));
```

### Edge case

1. f either input map is NULL, the result is NULL.
2. If both maps are empty {}, result is {}.

### Common patterns

#### 1. Sum/merge counts per key

```sql
WITH sample AS (
  SELECT
    map('a', 1, 'b', 2)  AS m1,
    map('b', 20, 'c', 30) AS m2
)
SELECT map_zip_with(
         m1, m2,
         (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0)
       ) AS merged
FROM sample;
-- Result: {"a":1, "b":22, "c":30}
```

#### Prefer non-null (fallback/overlay)

```sql
SELECT map_zip_with(
         m1, m2,
         (k, v1, v2) -> coalesce(v2, v1)
       ) AS overlaid
FROM sample;
-- Result: {"a":1, "b":20, "c":30}
```

#### Compute differences (e.g., deltas)

```sql
SELECT map_zip_with(
         m1, m2,
         (k, v1, v2) -> coalesce(v2, 0) - coalesce(v1, 0)
       ) AS delta
FROM sample;
-- Result: {"a":-1, "b":18, "c":30}
```

#### Build complex values (structs) per key

```sql
SELECT map_zip_with(
         m1, m2,
         (k, v1, v2) -> named_struct('old', v1, 'new', v2, 'changed', v1 <> v2)
       ) AS diff_map
FROM sample;
```

#### Filter while zipping

```sql
WITH zipped AS (
  SELECT map_zip_with(m1, m2, (k, v1, v2) -> coalesce(v1,0) + coalesce(v2,0)) AS m
  FROM sample
)
SELECT map_filter(m, (k, v) -> v > 10) AS filtered FROM zipped;
-- Keep only keys whose combined value > 10 → {"b":22, "c":30}
```

#### Edge cases & behavior

1. Missing keys: For a key present in only one map, the other value is NULL. Plan to use coalesce if you're doing arithmetic or logical ops.
2. NULL maps: If m1 or m2 is NULL, the whole result is NULL.
3. Value types: Works with any consistent value type (numbers, strings, structs, arrays, etc.). The lambda’s return type defines the result map’s value type.
4. Duplicate keys: MapType enforces unique keys. If you created a map with duplicate keys (e.g., via map_from_arrays with duplicate keys), Spark keeps the last occurrence. map_zip_with then acts on that canonicalized map.
5. Key order: Maps are unordered; don't rely on iteration order.

#### When to use vs. alternatives

1. map_zip_with: Best for per-key computations across two maps without exploding rows.
2. map_concat: Just concatenates; later values overwrite earlier keys. No per-key logic.
3. transform_values: Transform values within one map (no union of keys).
4. aggregate/map_from_entries with explode: Useful when you need complex multi-map logic across many maps or need to filter/drop keys mid-flight; comes with a shuffle/row expansion cost.


More examples (realistic)
Merge partial day metrics
sql
Copy
Edit
-- m_am: {"clicks": 10, "imps": 100}
-- m_pm: {"clicks": 12, "imps": 90, "cost": 5.5}
SELECT map_zip_with(
         m_am, m_pm,
         (k, a, p) -> coalesce(a, 0) + coalesce(p, 0)
       ) AS daily
FROM events;
-- {"clicks":22, "imps":190, "cost":5.5}
Compute percent change where both sides exist; otherwise keep NULL
sql
Copy
Edit
SELECT map_zip_with(
         m_prev, m_curr,
         (k, pv, cv) ->
           CASE WHEN pv IS NOT NULL AND cv IS NOT NULL AND pv <> 0
                THEN (cv - pv) / pv
                ELSE NULL
           END
       ) AS pct_change
FROM t;
Keep only keys that changed (two-step)
sql
Copy
Edit
WITH zipped AS (
  SELECT map_zip_with(m1, m2, (k, v1, v2) -> IF(v1 <> v2, 1, NULL)) AS flag_map
  FROM t
)
SELECT map_filter(flag_map, (k, v) -> v IS NOT NULL) AS changed_keys
FROM zipped;
-- map whose keys are only those that changed