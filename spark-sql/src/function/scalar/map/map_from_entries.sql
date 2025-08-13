--  Create key-value rows table

CREATE OR REPLACE TEMP VIEW key_value_table AS
(
    SELECT
        'name' AS key,
        'John' AS value
    UNION ALL
    SELECT
        'age',
        '30'
    UNION ALL
    SELECT
        'city',
        'Boston'
);

SELECT
    map_from_entries(
        collect_list(named_struct('key', key, 'value', value))
    ) AS result_map
FROM key_value_table;

SELECT map_from_entries(array(struct(1, 'a'), struct(2, 'b')));
