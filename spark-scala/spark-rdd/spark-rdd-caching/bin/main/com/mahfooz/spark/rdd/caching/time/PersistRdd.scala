package com.mahfooz.spark.rdd.caching.time

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object PersistRdd {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(PersistRdd.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df =
      spark.read.option("header", true).csv("D:\\data\\spark\\csv\\people.csv")

    df.persist(StorageLevel.DISK_ONLY)

    for (_ <- 0 until 10) {
      spark.time(df.filter($"name" like "%org%").count)
    }

  }

}
