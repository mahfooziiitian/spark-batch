# Map function

## element_at

    element_at(array, index)

Returns element of array at given (1-based) index.
If Index is 0, Spark will throw an error.
If index < 0, accesses elements from the last to the first.

    SELECT element_at(map(1, 'a', 2, 'b'), 2);

## map

    map(key0, value0, key1, value1, ...)

Creates a map with the given key/value pairs.

    SELECT map(1.0, '2', 3.0, '4');

## map_concat

    map_concat(map, ...)

Returns the union of all the given maps

    SELECT map_concat(map(1, 'a', 2, 'b'), map(3, 'c'));

## map_contains_key

map_contains_key(map, key)

Returns true if the map contains the key.

    SELECT map_contains_key(map(1, 'a', 2, 'b'), 1);
    SELECT map_contains_key(map(1, 'a', 2, 'b'), 3);


## map_entries

    map_entries(map)

Returns an unordered array of all entries in the given map.

    SELECT map_entries(map(1, 'a', 2, 'b'));

## map_filter

    map_filter(expr, func)

Filters entries in a map using the function.

    SELECT map_filter(map(1, 0, 2, 2, 3, -1), (k, v) -> k > v);



## map_from_arrays

    map_from_arrays(keys, values)

Creates a map with a pair of the given key/value arrays. All elements in keys should not be null
    
    SELECT map_from_arrays(array(1.0, 3.0), array('2', '4'));

## map_from_entries

    map_from_entries(arrayOfEntries)

Returns a map created from the given array of entries.

    SELECT map_from_entries(array(struct(1, 'a'), struct(2, 'b')));
 
## map_keys

    map_keys(map)

Returns an unordered array containing the keys of the map.

    SELECT map_keys(map(1, 'a', 2, 'b'));

## map_values

    map_values(map)

Returns an unordered array containing the values of the map.

    SELECT map_values(map(1, 'a', 2, 'b'));

## map_zip_with

    map_zip_with(map1, map2, function)

Merges two given maps into a single map by applying function to the pair of values with the same key. For keys only presented in one map, NULL will be passed as the value for the missing key. If an input map contains duplicated keys, only the first entry of the duplicated key is passed into the lambda function.

    SELECT map_zip_with(map(1, 'a', 2, 'b'), map(1, 'x', 2, 'y'), (k, v1, v2) -> concat(v1, v2));
    SELECT map_zip_with(map('a', 1, 'b', 2), map('b', 3, 'c', 4), (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0));
