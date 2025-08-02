-- 1. Create table for column details
DROP TABLE IF EXISTS products;

-- 2. Create table with columns and comments
CREATE TABLE products (
  id INT COMMENT 'Unique product ID',
  name STRING COMMENT 'Product name'
);

-- 3. SQL Commands to Get Column Details
DESCRIBE TABLE products;
DESCRIBE TABLE EXTENDED products;

-- 4. SQL Commands to Get Column Names
SELECT column_name
FROM system.information_schema.columns
WHERE table_name = 'products';

-- 5. SQL Commands to Get Column Data Types
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'products';

-- 6. SQL Commands to Get Column Constraints
SELECT column_name, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 'products';

