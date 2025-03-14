# array

    SELECT 
        array_agg(col) 
    FROM 
        VALUES (1), (2), (1) AS tab(col);
