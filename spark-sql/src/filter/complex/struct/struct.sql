-- Struct

CREATE OR REPLACE TEMP VIEW complex_sales AS
(
    SELECT
        name,
        location,
        tags,
        info
    FROM
        VALUES
        (
            'Alice',
            STRUCT('NY' as city, 'USA' as country),
            ARRAY('gift', 'priority'),
            MAP('product', 'A', 'price', '100')
        ),
        (
            'Bob',
            STRUCT('LA' as city, 'USA' as country),
            ARRAY('discount'),
            MAP('product', 'B', 'price', '200')
        ),
        (
            'Charlie',
            STRUCT('Toronto' as city, 'Canada' as country),
            ARRAY('gift', 'exclusive'),
            MAP('product', 'C', 'price', '300')
        ),
        (
            'Dana',
            STRUCT('Vancouver' as city, 'Canada' as country),
            ARRAY('promo'),
            MAP('product', 'A', 'price', '250')
        )
        AS complex_sales (name, location, tags, info)
);

-- Get all customers in USA
SELECT * FROM complex_sales
WHERE `location`.`country` = 'USA';

-- Customers from NY only
SELECT * FROM complex_sales
WHERE `location`.`city` = 'NY';
