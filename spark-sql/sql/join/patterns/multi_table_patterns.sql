-- Demonstrates joining many tables, using multiple join fields, and intermediate table join techniques.

-- =============================================================================
-- Section 1: Full Chain from Make Through to Customer
-- =============================================================================

SELECT
    mk.makename,
    md.modelname,
    st.color,
    sd.saleprice,
    sa.saledate,
    cu.customername
FROM make AS mk
INNER JOIN model AS md
    ON mk.makeid = md.makeid
INNER JOIN stock AS st
    ON md.modelid = st.modelid
INNER JOIN salesdetails AS sd
    ON st.stockcode = sd.stockid
INNER JOIN sales AS sa
    ON sd.salesid = sa.salesid
INNER JOIN customer AS cu
    ON sa.customerid = cu.customerid
ORDER BY sa.saledate, mk.makename;

-- =============================================================================
-- Section 2: Multiple Fields in JOIN — Composite Key
-- =============================================================================

SELECT
    a1.customername,
    a1.makename,
    a1.color,
    a1.saledate AS first_purchase,
    a2.saledate AS repeat_purchase
FROM allsales AS a1
INNER JOIN allsales AS a2
    ON
        a1.customername = a2.customername
        AND a1.makename = a2.makename
        AND a1.color = a2.color
        AND a1.saledate < a2.saledate
ORDER BY a1.customername, a1.makename;

-- =============================================================================
-- Section 3: Intermediate Table Join — Many-to-Many Bridge
-- =============================================================================

WITH sale_features AS (
    SELECT
        1 AS sale_id,
        'Leather Seats' AS feature
    UNION ALL
    SELECT
        1 AS sale_id,
        'Sunroof' AS feature
    UNION ALL
    SELECT
        2 AS sale_id,
        'Sunroof' AS feature
    UNION ALL
    SELECT
        3 AS sale_id,
        'Leather Seats' AS feature
    UNION ALL
    SELECT
        3 AS sale_id,
        'Navigation' AS feature
),

sale_header AS (
    SELECT
        1 AS sale_id,
        'Ferrari' AS makename,
        65000.00 AS saleprice
    UNION ALL
    SELECT
        2 AS sale_id,
        'Bentley' AS makename,
        90000.00 AS saleprice
    UNION ALL
    SELECT
        3 AS sale_id,
        'Rolls Royce' AS makename,
        140000.00 AS saleprice
)

SELECT
    sh.makename,
    sh.saleprice,
    sf.feature
FROM sale_header AS sh
INNER JOIN sale_features AS sf
    ON sh.sale_id = sf.sale_id
ORDER BY sh.makename, sf.feature;

-- =============================================================================
-- Section 4: Filtering via Joins Instead of Subquery
-- =============================================================================

WITH top_makes AS (
    SELECT makename
    FROM allsales
    GROUP BY makename
    HAVING SUM(saleprice) > 200000
)

SELECT
    a.makename,
    a.saledate,
    a.saleprice
FROM allsales AS a
INNER JOIN top_makes AS tm
    ON a.makename = tm.makename
ORDER BY a.makename, a.saledate;
