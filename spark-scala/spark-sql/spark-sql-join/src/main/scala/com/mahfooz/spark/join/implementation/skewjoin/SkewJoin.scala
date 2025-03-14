package com.mahfooz.spark.join.implementation.skewjoin

object SkewJoin {

  def main(args: Array[String]): Unit = {
    /*
    Skew join hints are not required. Skew is automatically taken care of if adaptive query execution (AQE) and spark.sql.adaptive.skewJoin.enabled are both enabled.
     */
/*
-- table with skew
SELECT /*+ SKEW('orders') */ * FROM orders, customers WHERE c_custId = o_custId

-- subquery with skew
SELECT /*+ SKEW('C1') */ *
  FROM (SELECT * FROM customers WHERE c_custId < 100) C1, orders
  WHERE C1.c_custId = o_custId
 */
    /**
     * Configure skew hint with relation name and column names
     */
    /*
    -- single column
SELECT /*+ SKEW('orders', 'o_custId') */ *
  FROM orders, customers
  WHERE o_custId = c_custId

-- multiple columns
SELECT /*+ SKEW('orders', ('o_custId', 'o_storeRegionId')) */ *
  FROM orders, customers
  WHERE o_custId = c_custId AND o_storeRegionId = c_regionId
     */
    /**
     * Configure skew hint with relation name, column names, and skew values
     */
    /*
-- single column, single skew value
SELECT /*+ SKEW('orders', 'o_custId', 0) */ *
  FROM orders, customers
  WHERE o_custId = c_custId

-- single column, multiple skew values
SELECT /*+ SKEW('orders', 'o_custId', (0, 1, 2)) */ *
  FROM orders, customers
  WHERE o_custId = c_custId

-- multiple columns, multiple skew values
SELECT /*+ SKEW('orders', ('o_custId', 'o_storeRegionId'), ((0, 1001), (1, 1002))) */ *
  FROM orders, customers
  WHERE o_custId = c_custId AND o_storeRegionId = c_regionId
     */
  }

}
