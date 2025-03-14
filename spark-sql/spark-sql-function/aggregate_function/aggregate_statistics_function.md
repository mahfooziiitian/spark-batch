# statistics function

## avg

    SELECT 
        avg(col) 
    FROM 
        VALUES (1), (2), (3) AS tab(col);

    SELECT 
        avg(col) 
    FROM 
        VALUES (1), (2), (NULL) AS tab(col);

## mean

    SELECT 
        mean(col) 
    FROM 
        VALUES (1), (2), (3) AS tab(col);

    SELECT 
        mean(col) 
    FROM 
        VALUES (1), (2), (NULL) AS tab(col);

## median

    SELECT 
        median(col) 
    FROM 
        VALUES (0), (10) AS tab(col);

    SELECT 
        median(col) 
    FROM 
        VALUES (INTERVAL '0' MONTH), (INTERVAL '10' MONTH) AS tab(col);

## min
    
    SELECT 
        min(col) 
    FROM 
        VALUES (10), (-1), (20) AS tab(col);

## min_by

    SELECT 
        min_by(x, y) 
    FROM 
        VALUES ('a', 10), ('b', 50), ('c', 20) AS tab(x, y);

## mode

    SELECT 
        mode(col) 
    FROM 
        VALUES (0), (10), (10) AS tab(col);

    SELECT 
        mode(col) 
    FROM 
        VALUES (INTERVAL '0' MONTH), (INTERVAL '10' MONTH), (INTERVAL '10' MONTH) AS tab(col);

    SELECT 
        mode(col) 
    FROM 
        VALUES (0), (10), (10), (null), (null), (null) AS tab(col);

## percentile

    SELECT 
        percentile(col, 0.3) 
    FROM 
        VALUES (0), (10) AS tab(col);

    SELECT 
        percentile(col, array(0.25, 0.75)) 
    FROM 
        VALUES (0), (10) AS tab(col);

    SELECT 
        percentile(col, 0.5) 
    FROM 
        VALUES (INTERVAL '0' MONTH), (INTERVAL '10' MONTH) AS tab(col);

    SELECT 
        percentile(col, array(0.2, 0.5)) 
    FROM 
        VALUES (INTERVAL '0' SECOND), (INTERVAL '10' SECOND) AS tab(col);
