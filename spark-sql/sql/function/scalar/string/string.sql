-- Basic usage
SELECT split('apple,banana,orange', ',') AS fruits;

-- Accessing specific element
SELECT split('apple,banana,orange', ',')[0] AS first_fruit;

-- Using regex delimiter
SELECT split('cat:dog;fish', '[:;]') AS animals;

-- With a table column
SELECT
    id,
    split(full_name, ' ')[0] AS first_name,
    split(full_name, ' ')[1] AS last_name
FROM customers;

-- application
SELECT
    explode(split(
        if(
            trim('<USE TAG FILTER>') = '<USE TAG FILTER>',
            'Budget;Env',
            'Tenant'
        ),
        ';'
    )) AS tag_entry,
    contains(tag_entry, '=') AS is_filter,
    if(is_filter, split(tag_entry, '=')[0], tag_entry) AS tag_key;

SELECT
    explode(split(
        if(
            trim('<USE TAG FILTER>1') = '<USE TAG FILTER>',
            'Budget;Env',
            'Tenant'
        ),
        ';'
    )) AS tag_entry,
    contains(tag_entry, '=') AS is_filter,
    if(is_filter, split(tag_entry, '=')[0], tag_entry) AS tag_key;

SELECT
    array_sort(collect_list(tag_key)) AS all_keys,
    array_sort(collect_list(if(is_filter, tag_key, NULL))) AS filter_keys,
    array_sort(collect_list(if(is_filter, tag_entry, NULL))) AS filter_expected
FROM (
    SELECT
        explode(split(
            if(
                trim('<USE TAG FILTER>') = '<USE TAG FILTER>',
                'Budget;Env',
                'Tenant'
            ),
            ';'
        )) AS tag_entry,
        contains(tag_entry, '=') AS is_filter,
        if(is_filter, split(tag_entry, '=')[0], tag_entry) AS tag_key
) AS tags
