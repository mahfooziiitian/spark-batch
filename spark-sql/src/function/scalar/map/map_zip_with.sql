SELECT
    map_zip_with(
        map(1, 'a', 2, 'b'),
        map(1, 'x', 2, 'y'),
        (k, v1, v2) -> concat(v1, v2)
    ) AS result_map;

SELECT
    map_zip_with(
        map('a', 1, 'b', 2),
        map('b', 3, 'c', 4),
        (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0)
    ) AS result_map;

-- ======================application======================
