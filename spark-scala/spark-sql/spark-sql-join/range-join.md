# Range join

A range join occurs when two relations are joined using a point in interval or interval overlap condition. The range join optimization support in Databricks Runtime can bring orders of magnitude improvement in query performance, but requires careful manual tuning.

## Point in interval range join
A point in interval range join is a join in which the condition contains predicates specifying that a value from one relation is between two values from the other relation.

    SELECT *
    FROM points JOIN ranges ON points.p BETWEEN ranges.start and ranges.end;

    -- using inequality expressions
    SELECT *
    FROM points JOIN ranges ON points.p >= ranges.start AND points.p < ranges.end;
    
    -- with fixed length interval
    SELECT *
    FROM points JOIN ranges ON points.p >= ranges.start AND points.p < ranges.start + 100;
    
    -- join two sets of point values within a fixed distance from each other
    SELECT *
    FROM points1 p1 JOIN points2 p2 ON p1.p >= p2.p - 10 AND p1.p <= p2.p + 10;
    
    -- a range condition together with other join conditions
    SELECT *
    FROM points, ranges
    WHERE points.symbol = ranges.symbol
    AND points.p >= ranges.start
    AND points.p < ranges.end;

## Interval overlap range join

An interval overlap range join is a join in which the condition contains predicates specifying an overlap of intervals between two values from each relation.

    -- overlap of [r1.start, r1.end] with [r2.start, r2.end]
    SELECT *
    FROM r1 JOIN r2 ON r1.start < r2.end AND r2.start < r1.end;
    
    -- overlap of fixed length intervals
    SELECT *
    FROM r1 JOIN r2 ON r1.start < r2.start + 100 AND r2.start < r1.start + 100;
    
    -- a range condition together with other join conditions
    SELECT *
    FROM r1 JOIN r2 ON r1.symbol = r2.symbol
    AND r1.start <= r2.end
    AND r1.end >= r2.start;

## Range join optimization

The range join optimization is performed for joins that:

    Have a condition that can be interpreted as a point in interval or interval overlap range join.
    All values involved in the range join condition are of a numeric type (integral, floating point, decimal), DATE, or TIMESTAMP.
    All values involved in the range join condition are of the same type. In the case of the decimal type, the values also need to be of the same scale and precision.
    It is an INNER JOIN, or in case of point in interval range join, a LEFT OUTER JOIN with point value on the left side, or RIGHT OUTER JOIN with point value on the right side.
    Have a bin size tuning parameter.

