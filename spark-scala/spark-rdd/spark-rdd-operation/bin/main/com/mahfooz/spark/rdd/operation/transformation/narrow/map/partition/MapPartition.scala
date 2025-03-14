package com.mahfooz.spark.rdd.operation.transformation.narrow.map.partition

import org.apache.spark.sql.SparkSession

object MapPartition {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("MapPartition")
      .master("local[*]")
      .getOrCreate()

    val sc=spark.sparkContext

    val rdd = sc.parallelize(1 to 3, 2)

    val z = rdd.mapPartitions(x => List(x.next).iterator)

    z.foreach(record=>println(record))
  }
}
