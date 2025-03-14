package com.mahfooz.ade.join

/**
 * AQE converts sort-merge join to broadcast hash join when the runtime statistics of any join side is smaller than the adaptive broadcast hash join threshold.
 * This is not as efficient as planning a broadcast hash join in the first place, but it’s better than keep doing the sort-merge join, as we can save the sorting of both the join sides, and read shuffle files locally to save network traffic(if spark.sql.adaptive.localShuffleReader.enabled is true)
 * spark.sql.adaptive.autoBroadcastJoinThreshold
 * spark.sql.adaptive.localShuffleReader.enabled
 */
object ConvertingSortMergeJoinToBroadcastJoin {
  def main(args: Array[String]): Unit = {

  }
}
