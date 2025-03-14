# Repartition

The "REPARTITION" hint has a partition number, columns, or both/neither of them as parameters.

The "REPARTITION_BY_RANGE" hint must have column names and a partition number is optional.

  SELECT /*+ REPARTITION(3) */ * FROM t;
  SELECT /*+ REPARTITION(c) */ * FROM t;
  SELECT /*+ REPARTITION(3, c) */ * FROM t;
  SELECT /*+ REPARTITION */ * FROM t;
  SELECT /*+ REPARTITION_BY_RANGE(c) */ * FROM t;
  SELECT /*+ REPARTITION_BY_RANGE(3, c)*/ * FROM t;
  