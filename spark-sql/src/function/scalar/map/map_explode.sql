CREATE OR REPLACE TEMP VIEW people_map AS
SELECT *
FROM
    VALUES (1, map('height', 180, 'weight', 75)),
    (2, map('height', 170, 'weight', 65)),
    (3, map('height', 160, 'weight', 55))
    AS (id, properties);

-- using lateral view explode to flatten the map
SELECT
    id,
    key,
    value
FROM people_map
    LATERAL VIEW explode(properties) as key, value;

-- using simple explode to flatten the map
SELECT
    id,
    explode(properties) AS (key, value)
FROM people_map;
