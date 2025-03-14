# rebalanced hint

* SELECT /*+ REBALANCE*/ * FROM t;
* SELECT /*+ REBALANCE(3)*/ * FROM t;
* SELECT /*+ REBALANCE(c)*/ * FROM t;
* SELECT /*+ REBALANCE(3, c)*/ * FROM t;
