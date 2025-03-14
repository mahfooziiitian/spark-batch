package com.mahfooz.spark.sql.caching.manager

import org.apache.spark.sql.SparkSession

object SparkSqlCacheManager {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

    val spark=SparkSession
      .builder()
      .appName("SparkSqlCacheManager")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    import spark.implicits._

    val q1 = spark.range(5).cache.filter($"id" % 2 === 0).select("id")
    
    val q2 = spark.range(1).filter($"id" % 2 === 0).select("id").cache

    //Be careful what you cache, i.e. what Dataset is cached, as it gives different queries cached.
    val cache = spark.sharedState.cacheManager

    println(cache.lookupCachedData(q2.queryExecution.logical).isDefined)

  }

}
