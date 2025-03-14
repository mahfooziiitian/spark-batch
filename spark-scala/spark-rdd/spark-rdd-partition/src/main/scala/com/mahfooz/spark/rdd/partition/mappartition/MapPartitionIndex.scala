/*
 mapPartitionsWithIndex is similar to mapPartitions() but it provides second parameter index which keeps the track of partition.

 */
package com.mahfooz.spark.rdd.partition.mappartition

import org.apache.spark.sql.SparkSession

object MapPartitionIndex {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(MapPartitionIndex.getClass.getName)
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(List(1, 2, 3, 4), 4)
    rdd
      .mapPartitionsWithIndex((index, iterator) => {
        iterator.map { x =>
          (index, x)
        }
      })
      .foreach { x =>
        println(x)
      }

    spark.stop()
  }
}
