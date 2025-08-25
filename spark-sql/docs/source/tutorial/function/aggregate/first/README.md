# first

    SELECT first(col) FROM VALUES (10), (5), (20) AS tab(col);
    
    SELECT first(col) FROM VALUES (NULL), (5), (20) AS tab(col);
    
    SELECT first(col, true) FROM VALUES (NULL), (5), (20) AS tab(col);

## first_value

    SELECT 
        first_value(col) 
    FROM 
        VALUES (10), (5), (20) AS tab(col);
    
    SELECT 
        first_value(col) 
    FROM 
        VALUES (NULL), (5), (20) AS tab(col);
    
    SELECT 
        first_value(col, true) 
    FROM 
        VALUES (NULL), (5), (20) AS tab(col);
