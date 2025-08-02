CREATE OR REPLACE TEMP VIEW sales_data AS
SELECT * FROM VALUES
  ('Alice', 'North', 'A', 100, 1, ARRAY('priority', 'gift')),
  ('Bob', 'North', 'B', 150, 2, ARRAY('discount')),
  ('Alice', 'South', 'A', 200, 3, ARRAY('clearance')),
  ('Bob', 'South', 'B', 300, NULL, ARRAY('gift')),
  ('Charlie', 'North', 'C', 400, 5, ARRAY('priority', 'exclusive')),
  ('Alice', 'North', NULL, 250, NULL, NULL),
  ('Charlie', 'South', 'A', 350, 4, ARRAY('gift'))
AS sales(name, region, product, amount, quantity, tags);

-- ✅ 1. Multi-Condition Filters with AND/OR/NULL Handling

SELECT * FROM sales_data
WHERE region = 'North'
  AND (product = 'A' OR product = 'B')
  AND quantity IS NOT NULL;
