-- Demonstrates a multi-dimensional PIVOT where both make and model act as
-- row groupings, and each distinct colour becomes its own column showing
-- the unit count of vehicles sold in that colour.
--
-- Key difference from a single-row PIVOT: PIVOT groups implicitly by every
-- non-pivoted, non-aggregated column in the source — here makename AND
-- modelname — producing one row per make/model combination.
--
-- Schema: allsales(makename, modelname, color, cost, saleprice, saledate)

-- ─────────────────────────────────────────────────────────────────────────────
-- Section 1: Core query — unit count per colour by make and model
-- ─────────────────────────────────────────────────────────────────────────────

-- The CTE narrows allsales to the four columns PIVOT needs.
-- COUNT(costprice) counts non-null cost values, so any row with a NULL cost
-- is excluded — intentional, as it flags incomplete stock records.
WITH make_model_color_cte AS (
    SELECT
        makename,
        modelname,
        color,
        cost AS costprice
    FROM allsales
)

SELECT
    makename,
    modelname,
    black,
    blue,
    british_racing_green,
    canary_yellow,
    dark_purple,
    green,
    night_blue,
    pink,
    red,
    silver
FROM make_model_color_cte
    PIVOT (
        COUNT(costprice) FOR color IN (
            'Black' black,
            'Blue' blue,
            'British Racing Green' british_racing_green,
            'Canary Yellow' canary_yellow,
            'Dark Purple' dark_purple,
            'Green' green,
            'Night Blue' night_blue,
            'Pink' pink,
            'Red' red,
            'Silver' silver
        )
    )
ORDER BY makename, modelname;

/* Expected shape (NULLs where no sale exists for that make/model/colour):
   makename     | modelname      | black | blue | british_racing_green | ... | red | silver
   -------------+----------------+-------+------+----------------------+-----+-----+-------
   Aston Martin | DB6            |     1 | NULL | NULL                 | ... |   2 |      1
   Bentley      | Continental    |     1 |    1 | NULL                 | ... | NULL|      1
   Bentley      | Mulsanne       |     2 | NULL |                    1 | ... | NULL|      1
   Ferrari      | 360            |     1 | NULL | NULL                 | ... |   2 |      1
   Rolls Royce  | Silver Shadow  |     1 |    1 | NULL                 | ... | NULL|      1
*/

-- ─────────────────────────────────────────────────────────────────────────────
-- Section 2: Replace NULL with zero — cleaner report for finance
-- ─────────────────────────────────────────────────────────────────────────────

WITH make_model_color_cte AS (
    SELECT
        makename,
        modelname,
        color,
        cost AS costprice
    FROM allsales
)

SELECT
    makename,
    modelname,
    COALESCE(black, 0) AS black,
    COALESCE(blue, 0) AS blue,
    COALESCE(british_racing_green, 0) AS british_racing_green,
    COALESCE(canary_yellow, 0) AS canary_yellow,
    COALESCE(dark_purple, 0) AS dark_purple,
    COALESCE(green, 0) AS green,
    COALESCE(night_blue, 0) AS night_blue,
    COALESCE(pink, 0) AS pink,
    COALESCE(red, 0) AS red,
    COALESCE(silver, 0) AS silver
FROM make_model_color_cte
    PIVOT (
        COUNT(costprice) FOR color IN (
            'Black' black,
            'Blue' blue,
            'British Racing Green' british_racing_green,
            'Canary Yellow' canary_yellow,
            'Dark Purple' dark_purple,
            'Green' green,
            'Night Blue' night_blue,
            'Pink' pink,
            'Red' red,
            'Silver' silver
        )
    )
ORDER BY makename, modelname;

-- ─────────────────────────────────────────────────────────────────────────────
-- Section 3: Sample data — verify the pivot with inline CTE rows
-- ─────────────────────────────────────────────────────────────────────────────

WITH allsales AS (
    SELECT
        'Ferrari' AS makename,
        '360' AS modelname,
        'Red' AS color,
        65000.00 AS cost
    UNION ALL
    SELECT
        'Ferrari' AS makename,
        '360' AS modelname,
        'Red' AS color,
        68000.00 AS cost
    UNION ALL
    SELECT
        'Ferrari' AS makename,
        '360' AS modelname,
        'Black' AS color,
        71000.00 AS cost
    UNION ALL
    SELECT
        'Ferrari' AS makename,
        '360' AS modelname,
        'Silver' AS color,
        64000.00 AS cost
    UNION ALL
    SELECT
        'Ferrari' AS makename,
        'Testarossa' AS modelname,
        'Red' AS color,
        82000.00 AS cost
    UNION ALL
    SELECT
        'Ferrari' AS makename,
        'Testarossa' AS modelname,
        'Black' AS color,
        85000.00 AS cost
    UNION ALL
    SELECT
        'Bentley' AS makename,
        'Mulsanne' AS modelname,
        'Black' AS color,
        95000.00 AS cost
    UNION ALL
    SELECT
        'Bentley' AS makename,
        'Mulsanne' AS modelname,
        'Black' AS color,
        98000.00 AS cost
    UNION ALL
    SELECT
        'Bentley' AS makename,
        'Mulsanne' AS modelname,
        'British Racing Green' AS color,
        92000.00 AS cost
    UNION ALL
    SELECT
        'Bentley' AS makename,
        'Mulsanne' AS modelname,
        'Silver' AS color,
        90000.00 AS cost
    UNION ALL
    SELECT
        'Bentley' AS makename,
        'Continental' AS modelname,
        'Black' AS color,
        115000.00 AS cost
    UNION ALL
    SELECT
        'Bentley' AS makename,
        'Continental' AS modelname,
        'Blue' AS color,
        112000.00 AS cost
    UNION ALL
    SELECT
        'Bentley' AS makename,
        'Continental' AS modelname,
        'Silver' AS color,
        110000.00 AS cost
    UNION ALL
    SELECT
        'Rolls Royce' AS makename,
        'Silver Shadow' AS modelname,
        'Black' AS color,
        145000.00 AS cost
    UNION ALL
    SELECT
        'Rolls Royce' AS makename,
        'Silver Shadow' AS modelname,
        'Night Blue' AS color,
        142000.00 AS cost
    UNION ALL
    SELECT
        'Rolls Royce' AS makename,
        'Silver Shadow' AS modelname,
        'Silver' AS color,
        140000.00 AS cost
),

make_model_color_cte AS (
    SELECT
        makename,
        modelname,
        color,
        cost AS costprice
    FROM allsales
)

SELECT
    makename,
    modelname,
    COALESCE(black, 0) AS black,
    COALESCE(blue, 0) AS blue,
    COALESCE(british_racing_green, 0) AS british_racing_green,
    COALESCE(night_blue, 0) AS night_blue,
    COALESCE(red, 0) AS red,
    COALESCE(silver, 0) AS silver
FROM make_model_color_cte
    PIVOT (
        COUNT(costprice) FOR color IN (
            'Black' black,
            'Blue' blue,
            'British Racing Green' british_racing_green,
            'Night Blue' night_blue,
            'Red' red,
            'Silver' silver
        )
    )
ORDER BY makename, modelname;

/* Expected output:
   makename    | modelname     | black | blue | british_racing_green | night_blue | red | silver
   ------------+---------------+-------+------+----------------------+------------+-----+-------
   Bentley     | Continental   |     1 |    1 |                    0 |          0 |   0 |      1
   Bentley     | Mulsanne      |     2 |    0 |                    1 |          0 |   0 |      1
   Ferrari     | 360           |     1 |    0 |                    0 |          0 |   2 |      1
   Ferrari     | Testarossa    |     1 |    0 |                    0 |          0 |   1 |      0
   Rolls Royce | Silver Shadow |     1 |    0 |                    0 |          1 |   0 |      1
*/
