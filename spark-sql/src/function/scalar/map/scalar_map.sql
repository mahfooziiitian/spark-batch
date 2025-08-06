--- 1. static map

SELECT map('name', 'Alice', 'age', '30', 'city', 'Paris') AS person_map;

--- 2. Create a map from column values
CREATE OR REPLACE TEMP VIEW person_data AS
SELECT
    1 AS id,
    'name' AS key1,
    'John' AS value1,
    'age' AS key2,
    '30' AS value2
UNION ALL
SELECT
    2,
    'name',
    'Alice',
    'age',
    '28';
SELECT
    id,
    map(key1, value1, key2, value2) AS person_map
FROM person_data;

--- 3. Create a map with NULL values
SELECT
    id,
    map(key1, value1, key2, NULL) AS person_map
FROM person_data;

--- 4. Create a map with dynamic keys and values
SELECT
    id,
    map(
        concat('key_', id),
        concat('value_', id),
        concat('key_', id + 1),
        concat('value_', id + 1)
    ) AS dynamic_map
FROM person_data;

--- 5. Create a map with multiple key-value pairs
SELECT
    id,
    map(
        key1, value1,
        key2, value2
    ) AS person_map
FROM person_data;

--- 6. Create a map with keys as expressions
SELECT
    id,
    map(
        concat('key_', id),
        concat('value_', id * 10),
        concat('key_', id + 1),
        concat('value_', (id + 1) * 10)
    ) AS expression_map
FROM person_data;

--- 7. Create a map with mixed static and dynamic keys
SELECT
    id,
    map(
        'static_key', 'static_value',
        concat('dynamic_key_', id),
        concat('dynamic_value_', id)
    ) AS mixed_map
FROM person_data;

--- 8. Create a map with keys from another table
SELECT
    t1.id,
    map(
        t2.key_column,
        t2.value_column
    ) AS map_from_another_table
FROM my_table AS t1
INNER JOIN another_table AS t2 ON t1.id = t2.id;
