# Set function

## collect_set

    collect_set(expr)

It collects and returns a set of unique elements.
The function is non-deterministic because the order of collected results depends on the order of
the rows which may be non-deterministic after a shuffle.

    SELECT 
        collect_set(col) 
    FROM 
        VALUES (1), (2), (1) AS tab(col);

## find_in_set

    find_in_set(str, str_array)

Returns the index (1-based) of the given string (str) in the comma-delimited list (str_array).

Returns 0, if the string was not found or if the given string (str) contains a comma.

    SELECT find_in_set('ab','abc,b,ab,c,def');
