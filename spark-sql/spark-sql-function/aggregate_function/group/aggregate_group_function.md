# last

## last

    SELECT last(col) FROM VALUES (10), (5), (20) AS tab(col);
    SELECT last(col) FROM VALUES (10), (5), (NULL) AS tab(col);
    SELECT last(col, true) FROM VALUES (10), (5), (NULL) AS tab(col);

## last_value

    SELECT last_value(col) FROM VALUES (10), (5), (20) AS tab(col);
    SELECT last_value(col) FROM VALUES (10), (5), (NULL) AS tab(col);
    SELECT last_value(col, true) FROM VALUES (10), (5), (NULL) AS tab(col);

