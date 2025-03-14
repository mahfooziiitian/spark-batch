/*

The number of partitions is important because this number directly influences the number of tasks that
will be running RDD transformations.
If the number of partitions is too small, then we will use only a few CPUs/cores on a lot of data thus
having a slower performance and leaving the cluster underutilized.

On the other hand, if the number of partitions is too large then you will use more resources than you
actually need and in a multi-tenant environment could be causing starvation of resources for other Jobs
being run by you or others in your team.

 */
package com.mahfooz.spark.rdd.shuffling

import org.apache.spark.sql.SparkSession

object DataShuffling2 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(DataShuffling2.getClass.getName)
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd_one = sc.parallelize(Seq(1, 2, 3))

    println(rdd_one.getNumPartitions)
  }

}
