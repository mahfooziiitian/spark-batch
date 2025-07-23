-- ✅ Sample Tables: Customers, Orders, Products
-- 1. customers

CREATE TABLE customers (
    customer_id STRING,
    name STRING,
    email STRING,
    country STRING
);

--- 2. orders

CREATE TABLE orders (
    order_id STRING,
    customer_id STRING,
    order_date DATE,
    total_amount DOUBLE
);

-- 3. products

CREATE TABLE products (
    product_id STRING,
    product_name STRING,
    price DOUBLE
);

-- 4. order_items

CREATE TABLE order_items (
    order_item_id STRING,
    order_id STRING,
    product_id STRING,
    quantity INT,
    unit_price DOUBLE
);

-- ✅ Sample Data (optional)
-- You can insert a few rows to make testing easier:

INSERT INTO customers VALUES
('cust_1', 'Alice', 'alice@example.com', 'US'),
('cust_2', 'Bob', 'bob@example.com', 'UK');

INSERT INTO orders VALUES
('ord_1', 'cust_1', '2024-01-10', 120.5),
('ord_2', 'cust_2', '2024-01-11', 220.0);

INSERT INTO products VALUES
('prod_1', 'Laptop', 1000),
('prod_2', 'Mouse', 25);

INSERT INTO order_items VALUES
('item_1', 'ord_1', 'prod_2', 2, 25),
('item_2', 'ord_2', 'prod_1', 1, 1000);