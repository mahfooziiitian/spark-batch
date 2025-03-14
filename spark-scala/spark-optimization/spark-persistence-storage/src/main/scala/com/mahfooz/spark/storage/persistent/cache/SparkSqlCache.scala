package com.mahfooz.spark.storage.persistent.cache

import org.apache.spark.sql.SparkSession

object SparkSqlCache {

  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .appName("SparkSqlCache")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.range(1 * 10000000).toDF("id")
      .withColumn("square", $"id" * $"id")

    df.createOrReplaceTempView("dfTable")

    var startTime = System.currentTimeMillis()
    println(df.count())
    println(s"time taken before cache = ${System.currentTimeMillis()-startTime}")

    startTime = System.currentTimeMillis()
    spark.sql("CACHE TABLE dfTable")
    println(s"time taken for cache = ${System.currentTimeMillis()-startTime}")

    startTime=System.currentTimeMillis()
    spark.sql("SELECT count(*) FROM dfTable").show()
    println(s"time taken after cache = ${System.currentTimeMillis()-startTime}")

    scala.io.StdIn.readLine()
  }

}
