-- Use with array literals

SELECT map_from_arrays(
    array('name', 'age', 'city'),
    array('Alice', '30', 'Paris')
) AS result_map;

-- Use with columns

CREATE OR REPLACE TEMP VIEW array_map_table AS
SELECT *
FROM
    VALUES
    (1, array('name', 'age', 'city'), array('John', '30', 'New York')),
    (2, array('name', 'age', 'city'), array('Alice', '28', 'London'))
AS array_map_table (id, keys, values);

SELECT
    id,
    map_from_arrays(keys, values) AS person_map
FROM array_map_table;
