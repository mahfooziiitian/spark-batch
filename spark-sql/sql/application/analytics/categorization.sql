-- Classifies data into ad hoc categories using CASE, IN lists, and nested classification logic.

-- =============================================================================
-- Section 1: Single Ad Hoc Category — Brand Tier
-- =============================================================================

SELECT
    makename,
    saleprice,
    CASE makename
        WHEN 'Ferrari' THEN 'Italian Supercar'
        WHEN 'Lamborghini' THEN 'Italian Supercar'
        WHEN 'Rolls Royce' THEN 'British Luxury'
        WHEN 'Bentley' THEN 'British Luxury'
        WHEN 'Aston Martin' THEN 'British Luxury'
        ELSE 'Other'
    END AS brand_tier
FROM allsales
ORDER BY brand_tier ASC, saleprice DESC;

-- =============================================================================
-- Section 2: Multiple Ad Hoc Categories Simultaneously
-- =============================================================================

SELECT
    makename,
    color,
    saleprice,
    CASE makename
        WHEN 'Ferrari' THEN 'Italian'
        WHEN 'Lamborghini' THEN 'Italian'
        ELSE 'Non-Italian'
    END AS origin,
    CASE
        WHEN saleprice >= 100000 THEN 'Premium'
        WHEN saleprice >= 60000 THEN 'Mid'
        ELSE 'Entry'
    END AS price_tier,
    CASE color
        WHEN 'Red' THEN 'Classic'
        WHEN 'Black' THEN 'Classic'
        WHEN 'Silver' THEN 'Classic'
        ELSE 'Distinctive'
    END AS colour_style
FROM allsales
ORDER BY makename, saleprice;

-- =============================================================================
-- Section 3: Inferring Customer Segments from Data
-- =============================================================================

SELECT
    customername,
    COUNT(*) AS purchase_count,
    SUM(saleprice) AS lifetime_value,
    CASE
        WHEN COUNT(*) >= 5 THEN 'VIP'
        WHEN COUNT(*) >= 3 THEN 'Loyal'
        WHEN COUNT(*) >= 2 THEN 'Returning'
        ELSE 'New'
    END AS customer_segment
FROM allsales
GROUP BY customername
ORDER BY lifetime_value DESC;

-- =============================================================================
-- Section 4: Nested Classification — Price AND Colour
-- =============================================================================

SELECT
    makename,
    color,
    saleprice,
    CASE
        WHEN saleprice >= 100000 THEN
            CASE color
                WHEN 'Black' THEN 'Premium Black'
                WHEN 'Silver' THEN 'Premium Silver'
                ELSE 'Premium Other'
            END
        ELSE
            CASE color
                WHEN 'Black' THEN 'Standard Black'
                WHEN 'Silver' THEN 'Standard Silver'
                ELSE 'Standard Other'
            END
    END AS category
FROM allsales
ORDER BY category ASC, saleprice DESC;

-- =============================================================================
-- Section 5: IN Operator for Lookup-Based Categorization
-- =============================================================================

SELECT
    makename,
    saleprice,
    CASE
        WHEN makename IN ('Ferrari', 'Lamborghini', 'Bugatti') THEN 'Supercar'
        WHEN makename IN ('Rolls Royce', 'Bentley', 'Maybach') THEN 'Ultra Luxury'
        WHEN makename IN ('Aston Martin', 'Maserati', 'Porsche') THEN 'Sports Luxury'
        ELSE 'Prestige'
    END AS vehicle_class
FROM allsales
ORDER BY vehicle_class ASC, saleprice DESC;

-- =============================================================================
-- Section 6: Sample Data
-- =============================================================================

WITH sample_data AS (
    SELECT
        'Ferrari' AS makename,
        'Red' AS color,
        65000.00 AS saleprice
    UNION ALL
    SELECT
        'Lamborghini' AS makename,
        'Yellow' AS color,
        110000.00 AS saleprice
    UNION ALL
    SELECT
        'Bentley' AS makename,
        'Black' AS color,
        90000.00 AS saleprice
    UNION ALL
    SELECT
        'Rolls Royce' AS makename,
        'Silver' AS color,
        155000.00 AS saleprice
    UNION ALL
    SELECT
        'Aston Martin' AS makename,
        'Green' AS color,
        75000.00 AS saleprice
)

SELECT
    makename,
    color,
    saleprice,
    CASE makename
        WHEN 'Ferrari' THEN 'Italian Supercar'
        WHEN 'Lamborghini' THEN 'Italian Supercar'
        WHEN 'Rolls Royce' THEN 'British Luxury'
        WHEN 'Bentley' THEN 'British Luxury'
        WHEN 'Aston Martin' THEN 'British Luxury'
        ELSE 'Other'
    END AS brand_tier,
    CASE
        WHEN saleprice >= 100000 THEN 'Premium'
        WHEN saleprice >= 60000 THEN 'Mid'
        ELSE 'Entry'
    END AS price_tier
FROM sample_data
ORDER BY brand_tier ASC, saleprice DESC;
