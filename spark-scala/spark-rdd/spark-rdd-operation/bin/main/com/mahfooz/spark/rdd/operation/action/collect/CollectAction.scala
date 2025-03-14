/*
All the elements of the data set are returned as an array to the driver program

 */
package com.mahfooz.spark.rdd.operation.action.collect

import org.apache.spark.sql.SparkSession

object CollectAction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CollectAction")
      .master("local")
      .getOrCreate()
    val numberRDD =
      spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)
    numberRDD.collect().foreach(println)
  }
}
