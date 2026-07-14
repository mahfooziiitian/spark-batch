-- ============================================================
-- Topic: Scalar functions — struct values
-- Dialect: Databricks / Spark SQL 3.5
-- Description: Demonstrates creating structs and reading struct fields.
-- ============================================================

-- Creating a struct
SELECT named_struct('name', 'Alice', 'age', 30) AS person;

SELECT struct('a', 1, 'b', 2, 'c', 3);

-- Accessing struct fields
SELECT
    person.name, --noqa: RF01
    person.age --noqa: RF01
FROM (
    SELECT named_struct('name', 'Alice', 'age', 30) AS person
);

-- Dot notation (most common)
SELECT
    person.name, --noqa: RF01
    person.age --noqa: RF01
FROM (
    SELECT named_struct('name', 'Alice', 'age', 30) AS person
);

-- Using element_at
SELECT element_at(person, 'name') AS person_name
FROM (
    SELECT named_struct('name', 'Alice', 'age', 30) AS person
);
