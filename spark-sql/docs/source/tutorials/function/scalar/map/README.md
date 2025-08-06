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

1. Merges two given maps into a single map by `applying function` to the pair of values with the `same key`.
2. For keys only presented in one map, NULL will be passed as the value for the missing key.
3. If an input map contains duplicated keys, only the first entry of the duplicated key is passed into the lambda function.

### Syntax map_zip_with

```sql
map_zip_with(map1, map2, function)
```

### Example map_zip_with

```sql
SELECT map_zip_with(map(1, 'a', 2, 'b'), map(1, 'x', 2, 'y'), (k, v1, v2) -> concat(v1, v2));
SELECT map_zip_with(map('a', 1, 'b', 2), map('b', 3, 'c', 4), (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0));
```
