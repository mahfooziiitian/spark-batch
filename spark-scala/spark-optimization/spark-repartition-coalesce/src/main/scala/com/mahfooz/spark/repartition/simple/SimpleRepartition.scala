/**
 * A simple repartition is a repartition who’s only parameter is target sPartition
 * count — IE: df.repartition(100).
 * In this case, a round-robin partitioner is used, meaning the only guarantee is that
 * the output data has roughly equally sized sPartitions.
 *
 * 1. A simple repartition can fix skewed data where the sPartitions are wildly different sizes.
 *
 *
 *
 */
package com.mahfooz.spark.repartition.simple

object SimpleRepartition {



}
