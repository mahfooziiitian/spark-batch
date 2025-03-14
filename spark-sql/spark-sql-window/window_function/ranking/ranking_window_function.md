# Ranking function

## Rank function

Computes the rank of a value in a group of values.

The result is one plus the number of rows preceding or equal to the current row 
in the ordering of the partition.

The values will produce gaps in the sequence.

    SELECT 
        name,
        dept,
        salary,
        RANK() OVER (PARTITION BY dept ORDER BY salary) AS rank 
    FROM 
        employees;

## dense_rank function

Computes the `rank` of a value in a group of values.

The result is one plus the previously assigned `rank` value.

Unlike the function rank, `dense_rank` will not produce gaps in the ranking sequence.

    SELECT 
        name,
        dept,
        salary,
        DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN
                            UNBOUNDED PRECEDING AND CURRENT ROW) AS dense_rank 
    FROM employees;

## percent_rank

Computes the percentage ranking of a value in a group of values.

        

## row_number

Assigns a unique, sequential number to each row, starting with one, according to 
the ordering of rows within the window partition.

    SELECT 
        a,
        b,
        row_number() OVER (PARTITION BY a ORDER BY b) 
    FROM 
        VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b);

    SELECT a,
         b,
         dense_rank() OVER(PARTITION BY a ORDER BY b),
         rank() OVER(PARTITION BY a ORDER BY b),
         row_number() OVER(PARTITION BY a ORDER BY b)
    FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b);


