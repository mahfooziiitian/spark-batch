package com.mahfooz.partition.repartition

object RepartitionHint {
  /**
   * SELECT /*+ REPARTITION(3) */ * FROM t;
   * SELECT /*+ REPARTITION(c) */ * FROM t;
   * SELECT /*+ REPARTITION(3, c) */ * FROM t;
   * SELECT /*+ REPARTITION */ * FROM t;
   * SELECT /*+ REPARTITION_BY_RANGE(c) */ * FROM t;
   * SELECT /*+ REPARTITION_BY_RANGE(3, c) */ * FROM t;
   */

}
