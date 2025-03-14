package com.mahfooz.spark.storage.persistent.persist

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object SparkCachePersist {

  def main(args: Array[String]): Unit = {

    val spark= SparkSession
      .builder()
      .appName("SparkCachePersist")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._


    // Create a DataFrame with 10M records
    val df = spark.range(1 * 10000000).toDF("id")
      .withColumn("square", $"id" * $"id")
    df.persist(StorageLevel.DISK_ONLY) // Serialize the data and cache it on disk

    var startTime = System.currentTimeMillis()
    println(df.count()) // Materialize the cache

    println(s"time taken = ${System.currentTimeMillis()-startTime}")

    startTime=System.currentTimeMillis()
    println(df.count())

    println(s"time taken = ${System.currentTimeMillis()-startTime}")

    startTime=System.currentTimeMillis()
    df.unpersist()

    println(s"time taken = ${System.currentTimeMillis()-startTime}")
    scala.io.StdIn.readLine()
  }

}
