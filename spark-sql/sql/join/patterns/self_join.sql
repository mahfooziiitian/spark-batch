-- Demonstrates self-joins where a table is joined to itself to compare rows within the same dataset.

-- =============================================================================
-- Section 1: Same-Make Cars Sold in the Same Month
-- =============================================================================

SELECT
    a1.makename,
    a1.saledate AS sale1_date,
    a1.saleprice AS sale1_price,
    a2.saledate AS sale2_date,
    a2.saleprice AS sale2_price
FROM allsales AS a1
INNER JOIN allsales AS a2
    ON
        a1.makename = a2.makename
        AND a1.saledate < a2.saledate
        AND DATE_TRUNC('MONTH', a1.saledate) = DATE_TRUNC('MONTH', a2.saledate)
ORDER BY a1.makename, a1.saledate;

-- =============================================================================
-- Section 2: Customers Who Bought More Than One Make
-- =============================================================================

SELECT DISTINCT
    a1.customername,
    a1.makename AS first_make,
    a2.makename AS second_make
FROM allsales AS a1
INNER JOIN allsales AS a2
    ON
        a1.customername = a2.customername
        AND a1.makename < a2.makename
ORDER BY a1.customername;

-- =============================================================================
-- Section 3: Hierarchical Self-Join (Manager / Employee)
-- =============================================================================

WITH staff AS (
    SELECT
        1 AS staff_id,
        'Alice' AS staff_name,
        CAST(NULL AS INT) AS manager_id,
        'CEO' AS job_title
    UNION ALL
    SELECT
        2 AS staff_id,
        'Bob' AS staff_name,
        1 AS manager_id,
        'Sales Director' AS job_title
    UNION ALL
    SELECT
        3 AS staff_id,
        'Carol' AS staff_name,
        2 AS manager_id,
        'Sales Manager' AS job_title
    UNION ALL
    SELECT
        4 AS staff_id,
        'Dave' AS staff_name,
        2 AS manager_id,
        'Account Manager' AS job_title
    UNION ALL
    SELECT
        5 AS staff_id,
        'Eve' AS staff_name,
        2 AS manager_id,
        'Account Manager' AS job_title
)

SELECT
    e.staff_name AS employee,
    e.job_title AS employee_role,
    COALESCE(m.staff_name, 'No Manager') AS manager
FROM staff AS e
LEFT JOIN staff AS m
    ON e.manager_id = m.staff_id
ORDER BY m.staff_name, e.staff_name;

-- =============================================================================
-- Section 4: Previous Period Comparison Self-Join
-- =============================================================================

WITH monthly AS (
    SELECT
        DATE_TRUNC('MONTH', saledate) AS sale_month,
        SUM(saleprice) AS monthly_revenue
    FROM allsales
    GROUP BY DATE_TRUNC('MONTH', saledate)
)

SELECT
    m1.sale_month,
    m1.monthly_revenue,
    m2.monthly_revenue AS prior_month_revenue,
    m1.monthly_revenue - COALESCE(m2.monthly_revenue, 0) AS month_change
FROM monthly AS m1
LEFT JOIN monthly AS m2
    ON m1.sale_month = DATE_ADD(m2.sale_month, 30)
ORDER BY m1.sale_month;
