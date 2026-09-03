-- Register as a temporary view
CREATE OR REPLACE TEMP VIEW sales_data AS
SELECT *
FROM (
    WITH base_data AS (
        SELECT
            date,
            region,
            product,
            amount
        FROM
            VALUES
            ('2025-01-01', 'North', 'Pen', 10),
            ('2025-01-01', 'North', 'Notebook', 20),
            ('2025-01-02', 'South', 'Pen', 5),
            ('2025-01-03', 'East', 'Pencil', 8),
            ('2025-01-04', 'South', 'Notebook', 15),
            ('2025-01-05', 'West', 'Pen', 12)
                AS sales (date, region, product, amount)
    )

    SELECT *
    FROM base_data
        LATERAL VIEW explode(sequence(1, 50000)) AS x
) AS expanded
