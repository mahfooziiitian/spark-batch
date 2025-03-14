package com.mahfooz.spark.sql.caching.cache

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object SparkCachingUsingTable {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse =
      sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse").replace("\\", "/")
    System.setProperty(
      "derby.system.home",
      sys.env.getOrElse("derby.system.home", "").replace("\\", "/")
    )

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkCachingUsingTable")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    spark.range(1).createOrReplaceTempView("one")

    // caching is lazy and won't happen until an action is executed
    val one = spark.table("one").cache

    println(spark.catalog.isCached("one"))

    one.show

    println(spark.catalog.isCached("one"))

    one.unpersist

    // caching is lazy
    spark.catalog.cacheTable("one", StorageLevel.MEMORY_ONLY)
    // The following gives "In-memory table one"
    one.show

    spark.range(100).createOrReplaceTempView("hundred")
    // SQL CACHE TABLE is eager
    // The following gives "In-memory table `hundred`"
    // WHY single quotes?
    spark.sql("CACHE TABLE hundred")

    // register Dataset under name
    val ds = spark.range(20)
    spark.sharedState.cacheManager.cacheQuery(ds, Some("twenty"))
    // trigger an action
    ds.head

  }
}
