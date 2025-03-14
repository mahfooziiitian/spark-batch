# aggregate set function

    SELECT 
        collect_set(col) 
    FROM 
        VALUES (1), (2), (1) AS tab(col);

