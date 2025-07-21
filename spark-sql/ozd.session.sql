
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('2024-01-01', 'North', 100),
  ('2024-01-02', 'North', 200),
  ('2024-01-05', 'North', 300),
  ('2024-01-08', 'North', 400),
  ('2024-01-10', 'North', 500)
AS sales(sale_date, region, amount)
-- SELECT *,
--   SUM(amount) OVER (
--     ORDER BY sale_date
--     RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
--   ) AS range_7d_total
-- FROM sales;