# Range join hint

## Enable range join using a range join hint

    SELECT /*+ RANGE_JOIN(points, 10)*/ *
    FROM points JOIN ranges ON points.p >= ranges.start AND points.p < ranges.end;

    SELECT /*+ RANGE_JOIN(r1, 0.1)*/ *
    FROM (SELECT* FROM ranges WHERE ranges.amount < 100) r1, ranges r2
    WHERE r1.start < r2.start + 100 AND r2.start < r1.start + 100;

    SELECT /*+ RANGE_JOIN(c, 500)*/ *
    FROM a
    JOIN b ON (a.b_key = b.id)
    JOIN c ON (a.ts BETWEEN c.start_time AND c.end_time)

SET spark.databricks.optimizer.rangeJoin.binSize=5

SELECT APPROX_PERCENTILE(CAST(end - start AS DOUBLE), ARRAY(0.5, 0.9, 0.99, 0.999, 0.9999)) FROM ranges

