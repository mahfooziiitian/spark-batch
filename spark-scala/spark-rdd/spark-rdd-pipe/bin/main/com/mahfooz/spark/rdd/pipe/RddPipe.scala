package com.mahfooz.spark.rdd.pipe

import org.apache.spark.sql.SparkSession

object RddPipe {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("RddPipe ")
      .master("local")
      .getOrCreate()

    val fileRDD=spark.sparkContext.wholeTextFiles(getClass
      .getResource("/spark_test.txt").getFile)

    val words=fileRDD.flatMap(line => line._2.split(" "))

    words.pipe("find /i \"spark\"").collect()
      .foreach(println)
  }
}
