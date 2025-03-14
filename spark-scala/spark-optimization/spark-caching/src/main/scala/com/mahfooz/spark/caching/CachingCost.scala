package com.mahfooz.spark.caching

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object CachingCost {

  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .appName("caching-cost")
      .master("local[*]")
      .getOrCreate()

    val dataframe = spark.range(1,10000000).toDF("id")
      .withColumn("square",col("id")* col("id"))

    var startTime = System.currentTimeMillis()
    dataframe.count()
    println(s"time taken = ${System.currentTimeMillis()-startTime}")

    startTime = System.currentTimeMillis()
    dataframe.persist()
    dataframe.count()
    println(s"time taken = ${System.currentTimeMillis()-startTime}")

  }

}
