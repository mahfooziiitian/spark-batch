package com.mahfooz.spark.rdd.operation.transformation.wide.keyvalue

import org.apache.spark.sql.SparkSession

object SparkByKey {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(SparkByKey.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val rdd=spark.sparkContext.wholeTextFiles(SparkByKey.getClass
      .getResource("/spark_test.txt").getFile)

    val words=rdd.flatMap(record => record._2.split(" "))
    val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)
    keyword.collect().foreach(println)
  }
}
