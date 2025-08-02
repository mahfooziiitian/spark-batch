-- Creating tables

CREATE TABLE  IF NOT EXISTS poc.testing.employees (
    id INT,
    name STRING,
    hire_date DATE
);

-- Inserting data
-- COMMAND ----
INSERT INTO poc.testing.employees(id, name, hire_date) VALUES (1, 'Alice', DATE('2022-01-15'));
INSERT INTO poc.testing.employees(id, name, hire_date ) VALUES (2, 'Bob', CAST('2022-02-01' AS DATE));

-- Select data
Select * from poc.testing.employees;

-- current date
Select current_date() as current_date;

-- drop date
DROP TABLE IF EXISTS poc.testing.employees;


