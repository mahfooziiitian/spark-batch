# spark join estimation

  drop table if exists t1
  drop table if exists t2
  
  REFRESH TABLE t1
  REFRESH TABLE t2

  ANALYZE TABLE t1 COMPUTE STATISTICS;
  ANALYZE TABLE t2 COMPUTE STATISTICS;
  