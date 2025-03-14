# Range join

A range join occurs when two relations are joined using a point in interval or interval overlap condition.

The range join optimization support in Databricks Runtime can bring orders of magnitude improvement in query performance, but requires careful manual tuning.

## Point in interval range join

A point in interval range join is a join in which the condition contains `predicates` specifying that `a value from one relation` is `between two values from the other relation`.

    SELECT *
    FROM points JOIN ranges ON points.p BETWEEN ranges.start and ranges.end;

### using inequality expressions

    SELECT 
        *
    FROM 
        points JOIN ranges ON points.p >= ranges.start AND points.p < ranges.end;

### with fixed length interval

    SELECT 
        *
    FROM 
        points JOIN ranges ON points.p >= ranges.start AND points.p < ranges.start + 100;

### join two sets of point values within a fixed distance from each other

    SELECT 
        *
    FROM 
        points1 p1 JOIN points2 p2 ON p1.p >= p2.p - 10 AND p1.p <= p2.p + 10;

### A range condition together with other join conditions

    SELECT 
        *
    FROM 
        points, ranges
    WHERE 
        points.symbol = ranges.symbol 
        AND points.p >= ranges.start
        AND points.p < ranges.end;

## Interval overlap range join

An `interval overlap range join` is a join in which the condition contains predicates specifying an `overlap of intervals between two values from each relation`.

### Overlap of [r1.start, r1.end] with [r2.start, r2.end]

    SELECT 
        *
    FROM 
        r1 JOIN r2 
        ON 
            r1.start < r2.end 
            AND r2.start < r1.end;

### Overlap of fixed length intervals

    SELECT 
        *
    FROM 
        r1 JOIN r2 
        ON 
            r1.start < r2.start + 100 
            AND r2.start < r1.start + 100;

### A range condition together with other join conditions in interval overlap join

    SELECT *
    FROM r1 JOIN r2 ON r1.symbol = r2.symbol
    AND r1.start <= r2.end
    AND r1.end >= r2.start;

## Range join optimization

The range join optimization is performed for joins that:

1. Have a condition that can be interpreted as `a point in interval or interval overlap range join`.
2. All values involved in the range join condition are of a `numeric type` (integral, floating point, decimal), DATE, or TIMESTAMP.
3. All values involved in the range join condition are of the `same type`. In the case of the decimal type, the values also need to be of the same scale and precision.
4. It is an `INNER JOIN`, or in case of point in interval range join, a `LEFT OUTER JOIN` with point value on the left side, or RIGHT OUTER JOIN with point value on the right side.
5. Have a bin size tuning parameter.

## Bin size

The bin size is a numeric tuning parameter that splits the values domain of the range condition into multiple bins of equal size.

For example, with a bin size of 10, the optimization splits the domain into bins that are intervals of length 10.

If you have a point in range condition of p BETWEEN start AND end, and start is 8 and end is 22, this value interval overlaps with three bins of length 10 – the first bin from 0 to 10, the second bin from 10 to 20, and the third bin from 20 to 30.

Only the points that fall within the same three bins need to be considered as possible join matches for that interval.

For example, if p is 32, it can be ruled out as falling between start of 8 and end of 22, because it falls in the bin from 30 to 40.

For DATE values, the value of the bin size is interpreted as days. For example, a bin size value of 7 represents a week.

For TIMESTAMP values, the value of the bin size is interpreted as seconds.

If a sub-second value is required, fractional values can be used.

For example, a bin size value of 60 represents a minute, and a bin size value of 0.1 represents 100 milliseconds

You can specify the bin size either by using a range join hint in the query or by setting a session configuration parameter. The range join optimization is applied only if you manually specify the bin size. Section Choose the bin size describes how to choose an optimal bin size.
