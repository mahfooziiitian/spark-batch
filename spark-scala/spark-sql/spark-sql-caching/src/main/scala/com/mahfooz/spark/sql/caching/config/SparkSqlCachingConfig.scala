package com.mahfooz.spark.sql.caching.config

import org.apache.spark.sql.SparkSession

object SparkSqlCachingConfig {

  val sparkWarehouse: String = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark-caching-config")
      .master("local[*]")
      .getOrCreate()

    val numDF = spark.range(100000).toDF("id")


    spark.sql("set spark.sql.inMemoryColumnarStorage.compressed=true")
    spark.sql("set spark.sql.inMemoryColumnarStorage.batchSize=10000")

    // register as a view
    numDF.createOrReplaceTempView("num_df")

    spark.sql("select * from num_df").show()
  }

}
