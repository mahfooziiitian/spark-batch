# count

    SELECT 
        approx_count_distinct(col1) 
    FROM 
        VALUES (1), (1), (2), (2), (3) tab(col1);
    
    SELECT 
        approx_percentile(col, array(0.5, 0.4, 0.1), 100) 
    FROM 
        VALUES (0), (1), (2), (10) AS tab(col);

## count_if

    SELECT 
        count_if(col % 2 = 0) 
    FROM 
        VALUES (NULL), (0), (1), (2), (3) AS tab(col);

    SELECT 
        count_if(col IS NULL) 
    FROM 
        VALUES (NULL), (0), (1), (2), (3) AS tab(col);

    
