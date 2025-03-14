package com.mahfooz.rdd.caching.storage.disk

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object PersistRddDiskOnly {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(PersistRddDiskOnly.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val dataPath =
      s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/Csv/people.csv"
    val df =
      spark.read.option("header", true).csv(dataPath)
    df.persist(StorageLevel.DISK_ONLY)

    println(df.storageLevel)

    for (_ <- 0 until 10) {
      spark.time(df.filter($"name" like "%org%").count)
    }
  }

}
