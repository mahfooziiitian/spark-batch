# max

## max

    SELECT 
        max(col) 
    FROM 
        VALUES (10), (50), (20) AS tab(col);


## max_by

    SELECT 
        max_by(x, y) 
    FROM 
        VALUES ('a', 10), ('b', 50), ('c', 20) AS tab(x, y);
