# Introduction

In Spark SQL, collection functions are used to work with complex data types like **arrays**, **maps**, and **structs**. These functions help in creating, accessing, and manipulating collections of data.

## Categories of Collection Functions

### 1. Array Functions

Operate on arrays (ordered lists of elements):

- `array()`: Creates an array.
- `size(array)`: Returns the number of elements.
- `array_contains(array, value)`: Checks if the array contains a value.
- `explode(array)`: Converts array elements into multiple rows.
- `sort_array(array)`: Sorts the array.
- `element_at(array, index)`: Gets the element at a specific index.
- `arrays_zip()`: Combines multiple arrays into a single array of structs.

### 2. Map Functions

Operate on key-value pairs:

- `map(key1, value1, ...)`: Creates a map.
- `map_keys(map)`: Returns an array of keys.
- `map_values(map)`: Returns an array of values.
- `element_at(map, key)`: Gets the value for a key.
- `size(map)`: Returns the number of entries.
- `explode(map)`: Converts map entries into rows.

### 3. Struct Functions

Operate on structs (named collections of fields):

- `named_struct(name1, value1, ...)`: Creates a struct.
- `get_json_object(json, path)`: Often used to extract struct-like data from JSON.
- `col.field`: Accesses a field in a struct.
