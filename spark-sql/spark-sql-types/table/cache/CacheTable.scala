package com.mhafooz.spark.sql.table.cache

import org.apache.spark.sql.SparkSession

object CacheTable {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CacheTable")
      .getOrCreate()

    val numDF = spark.range(1000).toDF("id")

    // register as a view
    numDF.createOrReplaceTempView("num_df")

    // use Spark catalog to cache the numDF using name "num_df"
    spark.catalog.cacheTable("num_df")

    // force the persistence to happen by taking the count action
    println(numDF.count)
  }
}
