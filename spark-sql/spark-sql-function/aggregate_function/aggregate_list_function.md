# aggregate list function

    SELECT 
        collect_list(col) 
    FROM 
        VALUES (1), (2), (1) AS tab(col);

