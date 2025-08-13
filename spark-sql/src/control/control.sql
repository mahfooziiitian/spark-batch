-- CASE WHEN
CREATE TABLE sales_table (
    name STRING,
    sales INT
);

INSERT INTO sales_table VALUES
('Alice', 120000),
('Bob', 75000),
('Charlie', 40000),
('David', 25000),
('Eva', 90000);

SELECT
    name,
    sales,
    CASE
        WHEN sales >= 100000 THEN 'High'
        WHEN sales >= 50000 THEN 'Medium'
        ELSE 'Low'
    END AS sales_category
FROM sales_table;

-- IF
CREATE TABLE people (
    name STRING,
    age INT
);

INSERT INTO people VALUES
('Alice', 25),
('Bob', 17),
('Charlie', 34),
('David', 15),
('Eva', 22);

SELECT
    name,
    IF(age >= 18, 'Adult', 'Minor') AS category
FROM people;
