-- Sample application database for JDBC Catalog demos

CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE,
    region VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    product VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    order_date DATE DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10, 2) NOT NULL,
    stock INTEGER DEFAULT 0
);

INSERT INTO customers (name, email, region) VALUES
    ('Alice Johnson', 'alice@example.com', 'North'),
    ('Bob Smith', 'bob@example.com', 'South'),
    ('Charlie Brown', 'charlie@example.com', 'East'),
    ('Diana Prince', 'diana@example.com', 'West'),
    ('Eve Wilson', 'eve@example.com', 'North');

INSERT INTO products (name, category, price, stock) VALUES
    ('Laptop', 'Electronics', 999.99, 50),
    ('Mouse', 'Electronics', 29.99, 200),
    ('Desk', 'Furniture', 349.99, 30),
    ('Chair', 'Furniture', 249.99, 45),
    ('Monitor', 'Electronics', 449.99, 75);

INSERT INTO orders (customer_id, product, quantity, price, order_date) VALUES
    (1, 'Laptop', 1, 999.99, '2024-01-15'),
    (1, 'Mouse', 2, 29.99, '2024-01-15'),
    (2, 'Desk', 1, 349.99, '2024-02-01'),
    (3, 'Chair', 2, 249.99, '2024-02-10'),
    (4, 'Monitor', 1, 449.99, '2024-03-01'),
    (5, 'Laptop', 1, 999.99, '2024-03-15'),
    (2, 'Mouse', 3, 29.99, '2024-03-20');
