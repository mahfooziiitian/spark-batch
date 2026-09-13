-- STRUCT type examples in Spark SQL (Databricks dialect).
-- Covers creation, field access, nested structs, reassembly, and INLINE flattening.

CREATE OR REPLACE TEMP VIEW customers AS
SELECT *
FROM
    VALUES
    (
        1,
        'Alice',
        STRUCT(
            'NY' as city,
            'USA' as country,
            STRUCT('10001' as zip, 'Manhattan' as district) as area
        )
    ),
    (
        2,
        'Bob',
        STRUCT(
            'LA' as city,
            'USA' as country,
            STRUCT('90001' as zip, 'Downtown' as district) as area
        )
    ),
    (
        3,
        'Carol',
        STRUCT(
            'Toronto' as city,
            'Canada' as country,
            STRUCT('M5V' as zip, 'Waterfront' as district) as area
        )
    ),
    (
        4,
        'Dana',
        STRUCT(
            'Vancouver' as city,
            'Canada' as country,
            STRUCT('V6B' as zip, 'Gastown' as district) as area
        )
    )
        AS customers (id, name, address);

CREATE OR REPLACE TEMP VIEW order_items AS
SELECT *
FROM
    VALUES
    (
        101,
        ARRAY(
            STRUCT('laptop' as item, 999.99 as price, 1 as qty),
            STRUCT('mouse' as item, 29.99 as price, 2 as qty)
        )
    ),
    (102, ARRAY(STRUCT('keyboard' as item, 79.99 as price, 1 as qty))),
    (
        103,
        ARRAY(
            STRUCT('monitor' as item, 349.99 as price, 1 as qty),
            STRUCT('cable' as item, 9.99 as price, 3 as qty)
        )
    )
        AS order_items (order_id, line_items);

---
-- 1. Creating structs
---

-- STRUCT() shorthand (field names become col0, col1, ... unless aliased)
SELECT STRUCT(1, 'hello', TRUE) AS anon_struct;

-- NAMED_STRUCT — explicit field names
SELECT NAMED_STRUCT('id', 42, 'label', 'widget', 'active', TRUE) AS named;

-- STRUCT literal with AS alias inside VALUES (idiomatic Spark SQL)
SELECT STRUCT('Paris' as city, 'France' as country) AS location;

---
-- 2. Dot-notation field access
---

SELECT
    customers.name,
    address.city,
    address.country
FROM customers;

---
-- 3. Filtering on struct field
---

-- All customers in USA
SELECT
    customers.name,
    address.city
FROM customers
WHERE address.country = 'USA';
-- Result: Alice (NY), Bob (LA)

---
-- 4. Nested struct access
---

SELECT
    customers.name,
    address.city,
    address.area.zip,
    address.area.district
FROM customers;

---
-- 5. Struct reassembly — update a single field
-- Pattern: reconstruct the struct, replacing only the changed field.
---

SELECT
    customers.id,
    customers.name,
    STRUCT(
        'UNKNOWN' as city,      -- overwrite city
        address.country as country,   -- keep original
        address.area as area       -- keep original
    ) AS address_anonymised
FROM customers;

-- Alternatively with NAMED_STRUCT
SELECT
    customers.id,
    customers.name,
    NAMED_STRUCT(
        'city', UPPER(address.city),  -- transform city to uppercase
        'country', address.country,
        'area', address.area
    ) AS address_upper_city
FROM customers;

---
-- 6. INLINE to flatten array of structs into rows
---

SELECT
    order_id,
    item,
    price,
    qty
FROM order_items
    LATERAL VIEW INLINE(line_items) as item, price, qty;

-- Or using the INLINE table function directly
SELECT
    t.*,
    order_items.order_id
FROM order_items, INLINE(order_items.line_items) AS t;

---
-- 7. Struct equality comparison
---

-- Structs are equal if all fields are equal
SELECT
    -- Result: true
    STRUCT('NY' as city, 'USA' as country)
    = STRUCT('NY' as city, 'USA' as country) AS same, -- noqa: ST10
    -- Result: false
    STRUCT('NY' as city, 'USA' as country) = STRUCT('LA' as city, 'USA' as country) AS different;

-- Find customers sharing the same country struct field value
SELECT
    a.name AS name_a,
    b.name AS name_b,
    a.address.country -- noqa: RF01
FROM customers AS a
INNER JOIN customers AS b
    ON
        a.address.country = b.address.country -- noqa: RF01
        AND a.id < b.id;

---
-- 8. CREATE TABLE with STRUCT column (DDL reference)
---

CREATE OR REPLACE TEMP VIEW customer_schema_demo AS
SELECT
    CAST(1 AS BIGINT) AS customer_id,
    CAST('Alice' AS STRING) AS name,
    STRUCT(
        CAST('123 Main St' AS STRING) as street,
        CAST('NY' AS STRING) as city,
        CAST('USA' AS STRING) as country
    ) AS address;

-- In a real Delta table the DDL would be:
-- CREATE TABLE customers (
--     customer_id BIGINT NOT NULL,
--     name        STRING,
--     address     STRUCT<street: STRING, city: STRING, country: STRING>
-- ) USING DELTA;

---
-- 9. TRANSFORM to update a field within array of structs
---

-- Increase every line-item price by 10 %
SELECT
    order_id,
    TRANSFORM(
        line_items,
        x
        -> STRUCT(
            x.item as item, ROUND(x.price * 1.1, 2) as price, x.qty as qty
        )
    ) AS line_items_adjusted
FROM order_items;
