# Count function

## count_if

    count_if(expr)

It returns the number of TRUE values for the expression.

Examples:

    SELECT count_if(col % 2 = 0) FROM VALUES (NULL), (0), (1), (2), (3) AS tab(col);
    SELECT count_if(col IS NULL) FROM VALUES (NULL), (0), (1), (2), (3) AS tab(col);

## count_min_sketch
count_min_sketch(col, eps, confidence, seed)

It Returns a count-min sketch of a column with the given esp, confidence and seed. The result is an array of bytes, which can be deserialized to a CountMinSketch before usage. Count-min sketch is a probabilistic data structure used for cardinality estimation using sub-linear space.
