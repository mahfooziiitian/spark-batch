package com.mahfooz.ade.skewjoin

/**
 * Data skew can severely downgrade the performance of join queries.
 * This feature dynamically handles skew in sort-merge join by splitting (and replicating if needed) skewed tasks into roughly evenly sized tasks.
 * It takes effect when both spark.sql.adaptive.enabled and spark.sql.adaptive.skewJoin.enabled configurations are enabled.
 *
 * spark.sql.adaptive.skewJoin.enabled
 * spark.sql.adaptive.skewJoin.skewedPartitionFactor
 * spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes
 * spark.sql.adaptive.forceOptimizeSkewedJoin
 *
 */
class OptimizingSkewJoin {

}
