package com.mahfooz.partition

/**
 * SELECT /*+ REBALANCE */ * FROM t;
 * SELECT /*+ REBALANCE(3) */ * FROM t;
 * SELECT /*+ REBALANCE(c) */ * FROM t;
 * SELECT /*+ REBALANCE(3, c) */ * FROM t;
 */
object RebalancedHint {
  def main(args: Array[String]): Unit = {

  }
}
