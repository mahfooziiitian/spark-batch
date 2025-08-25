SELECT map_filter(m, (k, v) -> v > 10) AS filtered_map
FROM
    VALUES (map(1, 5, 2, 15, 3, 25)) AS t (m);

SELECT map_filter(m, (k, v) -> k % 2 = 0) AS even_key_map
FROM
    VALUES (map(1, 'a', 2, 'b', 3, 'c', 4, 'd')) AS t (m);

-- Filtering with both key & value logic
SELECT map_filter(m, (k, v) -> k > 1 AND v LIKE 'b%') AS filtered
FROM
    VALUES (map(1, 'apple', 2, 'banana', 3, 'berry', 4, 'cherry')) AS t (m);

-- Filtering JSON-like map
SELECT
    id,
    map_filter(tags, (k, v) -> k IN ('env', 'version')) AS important_tags
FROM my_table;

-- Filter metadata / attributes
WITH attribute_data AS (
    SELECT
        1 AS id,
        map('env', 'prod', 'version', '1.2', 'owner', 'teamA') AS attributes
    UNION ALL
    SELECT
        2 AS id,
        map('env', 'dev', 'version', '2.0', 'owner', 'teamB') AS attributes
)

SELECT
    id,
    map_filter(attributes, (k, v) -> k IN ('env', 'version')) AS filtered
FROM attribute_data;

-- Drop null / empty values
SELECT map_filter(properties, (k, v) -> v IS NOT NULL AND v != '') AS cleaned
FROM
    VALUES (map('a', 'x', 'b', NULL, 'c', '')) AS t (properties);

WITH dataset AS (
    SELECT array('env','version','owner') AS keys,
           array('prod','1.2','teamA') AS values
)
SELECT map_zip_with(keys, values, (k, v) -> v) AS attributes
FROM dataset;

--transform_keys

--transform_values
