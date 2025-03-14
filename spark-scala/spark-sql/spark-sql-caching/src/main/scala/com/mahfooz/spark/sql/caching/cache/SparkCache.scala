
package com.mahfooz.spark.sql.caching.cache

import org.apache.spark.sql.SparkSession

object SparkCache {

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
      .appName("RefreshTableMetadata")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    val data = spark.range(1)

    data.createOrReplaceTempView("data")

    spark.sql("cache table data")

    spark.sql("select * from data").count()

    println(spark.sql("select * from data").queryExecution.withCachedData.numberedTreeString)

    spark.sql("select id as newId,id from data").explain(extended = true)

    println(spark.sql("select id as newId,id from data").queryExecution.withCachedData.numberedTreeString)

    spark.sql("CLEAR CACHE").collect.foreach(record=>println(record.get(1)))

  }

}
