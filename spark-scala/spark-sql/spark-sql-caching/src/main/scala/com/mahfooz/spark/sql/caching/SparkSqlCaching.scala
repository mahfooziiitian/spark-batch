package com.mahfooz.spark.sql.caching

import org.apache.spark.sql.SparkSession

object SparkSqlCaching {

  def main(args: Array[String]): Unit = {

    // warehouseLocation points to the default location for managed databases and tables
    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

    val spark=SparkSession
      .builder()
      .appName("SparkSqlCaching")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val numDF = spark.range(1000000).toDF("id")

    // register as a view
    numDF.createOrReplaceTempView("num_df")
    var starTime = System.nanoTime()
    spark.sql("select count(*) from num_df").show(false)
    var endTime = System.nanoTime()
    println(s"time taken = ${(endTime-starTime)/1000000}")

    spark.sql("cache table num_df")

    starTime = System.nanoTime()
    // force the persistence to happen by taking the count action
    spark.sql("select count(*) from num_df").show(false)
    endTime = System.nanoTime()

    println(s"time taken = ${(endTime-starTime)/1000000}")

    spark.sql("uncache table num_df")

    spark.wait(60)

  }

}
