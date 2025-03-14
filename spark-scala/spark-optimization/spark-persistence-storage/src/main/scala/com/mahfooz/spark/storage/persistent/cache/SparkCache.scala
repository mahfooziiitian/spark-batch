package com.mahfooz.spark.storage.persistent.cache

import org.apache.spark.sql.SparkSession

object SparkCache {

  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .appName("spark-cache")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.range(1 * 10000000).toDF("id")
      .withColumn("square", $"id" * $"id")

    df.cache() // Cache the data
    var startTime = System.currentTimeMillis()
    println(df.count()) // Materialize the cache

    println(s"time taken = ${System.currentTimeMillis()-startTime}")

    startTime=System.currentTimeMillis()
    println(df.count())

    println(s"time taken = ${System.currentTimeMillis()-startTime}")

    scala.io.StdIn.readLine()
  }

}
