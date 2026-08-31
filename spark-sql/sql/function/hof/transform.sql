-- transform(array, element -> expression) format
SELECT transform(array(1, 2, 3), x -> x * 2) AS doubled_array;

SELECT transform(
    array(1, 2, 3, 4),
    x -> CASE WHEN x < 3 THEN 0 ELSE x END
) AS conditioned_array;

SELECT transform(
    array(
        named_struct('name', 'Alice', 'age', 25),
        named_struct('name', 'Bob', 'age', 30)
    ),
    s -> named_struct('name', s.name, 'age', s.age + 1)
) AS transformed_structs;

-- transform(array, (element, index) -> expression) format
SELECT transform(array(1, 2, 3), (x, i) -> x + i) AS indexed_array;
