-- Demonstrates applying window functions over aggregated (grouped) results.
-- Combines GROUP BY for monthly totals with cumulative window functions.
-- Schema: allsales(makename, saleprice, saledate)

-- =============================================================================
-- Section 1: Monthly totals CTE then cumulative window over the result
-- =============================================================================
WITH monthly_totals AS (
    SELECT
        DATE_TRUNC('MONTH', saledate) AS sale_month,
        SUM(saleprice) AS monthly_total
    FROM allsales
    GROUP BY DATE_TRUNC('MONTH', saledate)
)

SELECT
    sale_month,
    monthly_total,
    SUM(monthly_total) OVER (
        ORDER BY sale_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_total
FROM monthly_totals
ORDER BY sale_month;

-- =============================================================================
-- Section 2: Per-make running total using PARTITION BY makename
-- =============================================================================
SELECT
    makename,
    saledate,
    saleprice,
    SUM(saleprice) OVER (
        PARTITION BY makename
        ORDER BY saledate
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS make_running_total
FROM allsales
ORDER BY makename, saledate;

-- =============================================================================
-- Section 3: Side-by-side per-make running total + overall running total
-- =============================================================================
SELECT
    makename,
    saledate,
    saleprice,
    SUM(saleprice) OVER (
        PARTITION BY makename
        ORDER BY saledate
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS make_running_total,
    SUM(saleprice) OVER (
        ORDER BY saledate
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS overall_running_total
FROM allsales
ORDER BY saledate, makename;

-- =============================================================================
-- Section 4: Per-make total revenue with cumulative percentage (empty OVER = grand total)
-- =============================================================================
WITH make_totals AS (
    SELECT
        makename,
        SUM(saleprice) AS total_revenue
    FROM allsales
    GROUP BY makename
)

SELECT
    makename,
    total_revenue,
    ROUND(
        SUM(total_revenue) OVER (
            ORDER BY total_revenue DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) * 100.0 / SUM(total_revenue) OVER (),
        2
    ) AS cumulative_pct
FROM make_totals
ORDER BY total_revenue DESC;
