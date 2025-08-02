# Math

## isnan

    isnan(expr)

Returns true if expr is NaN, or false otherwise.

    SELECT isnan(cast('NaN' as double));

## Number format

    SELECT to_number('-$12,345.67', 'S$999,099.99');
    SELECT to_number('5', '$9');
    SELECT to_number('$45', 'S$999,099.99');
    SELECT to_number('1234-', '999999MI');
