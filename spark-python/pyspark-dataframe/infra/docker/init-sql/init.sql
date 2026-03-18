-- MySQL initialisation script — executed once on first container start.
-- Matches the sample_data.py datasets so io/database examples work out of the box.

CREATE DATABASE IF NOT EXISTS datascience;
USE datascience;

-- ── employees ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS employees (
    id             INT         NOT NULL AUTO_INCREMENT,
    employee_name  VARCHAR(100),
    department_id  INT,
    PRIMARY KEY (id)
);

INSERT INTO employees (employee_name, department_id) VALUES
    ('Homer Simpson',  4),
    ('Ned Flanders',   1),
    ('Barney Gumble',  5),
    ('Clancy Wiggum',  3),
    ('Moe Syzslak',    NULL),
    ('Lisa Simpson',   2);

-- ── departments ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS departments (
    department_id    INT         NOT NULL AUTO_INCREMENT,
    department_name  VARCHAR(100),
    PRIMARY KEY (department_id)
);

INSERT INTO departments (department_name) VALUES
    ('Sales'),
    ('Engineering'),
    ('Human Resources'),
    ('Customer Service'),
    ('Research And Development');

-- ── salaries ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS salaries (
    employee_id     INT     NOT NULL,
    current_salary  DECIMAL(10, 2),
    PRIMARY KEY (employee_id)
);

INSERT INTO salaries (employee_id, current_salary) VALUES
    (1, 60000.00),
    (2, 75000.00),
    (3, 50000.00),
    (4, 82000.00),
    (6, 90000.00);

-- ── customer_orders ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customer_orders (
    order_id    INT           NOT NULL,
    customer_id INT,
    product     VARCHAR(100),
    quantity    INT,
    unit_price  DECIMAL(10, 2),
    status      VARCHAR(50),
    PRIMARY KEY (order_id)
);

INSERT INTO customer_orders (order_id, customer_id, product, quantity, unit_price, status) VALUES
    (1001, 1,    'Widget', 3,  9.99,  'active'),
    (1002, 2,    'Gadget', 1,  49.99, 'active'),
    (1003, 1,    'Widget', 5,  9.99,  'active'),
    (1004, 3,    'Book',   10, 14.99, 'inactive'),
    (1005, 2,    'Gadget', 2,  49.99, 'active'),
    (1006, 4,    'Widget', 7,  9.99,  'active'),
    (1007, NULL, 'Book',   3,  14.99, 'active');
