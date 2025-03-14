package com.mahfooz.spark.rdd.operation.transformation.narrow.map.partition

import org.apache.spark.sql.SparkSession

object MapPartitionWithIndex {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("MapPartitionWithIndex")
      .master("local")
      .getOrCreate()

    //It will generate data with two partitions.
    val numberRDD =
      spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)

    numberRDD
      .mapPartitionsWithIndex((idx: Int, itr: Iterator[Int]) => {
        itr.map(n => (idx, n))
      })
      .collect()
      .foreach(println)

  }
}
