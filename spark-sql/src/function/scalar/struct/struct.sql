--  Creating a struct
SELECT named_struct('name', 'Alice', 'age', 30) AS person;

SELECT struct('a', 1, 'b', 2, 'c', 3);

-- Accessing struct fields
SELECT
    person.name,
    person.age
FROM (
    SELECT named_struct('name', 'Alice', 'age', 30) AS person
);

-- Dot notation (most common)

SELECT
    person.name,
    person.age
FROM (
    SELECT named_struct('name', 'Alice', 'age', 30) AS person
);

-- Using element_at

SELECT element_at(person, 'name') AS person_name
FROM (
    SELECT named_struct('name', 'Alice', 'age', 30) AS person
);
