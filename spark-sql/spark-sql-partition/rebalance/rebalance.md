# rebalance

The "REBALANCE" hint has an initial partition number, columns, or both/neither of them as parameters.

    SELECT /*+ REBALANCE */ * FROM t;
    SELECT /*+ REBALANCE(3) */ * FROM t;
    SELECT /*+ REBALANCE(c) */ * FROM t;
    SELECT /*+ REBALANCE(3, c)*/ * FROM t;
