# List function

## collect_list

    collect_list(expr)

It collects and returns a list of non-unique elements.

The function is non-deterministic because the order of collected results depends on the order of the
rows which may be non-deterministic after a shuffle.

    SELECT collect_list(col) 
    FROM VALUES (1), (2), (1) AS tab(col);
