package com.mahfooz.spark.df.persistence

import org.apache.spark.sql.SparkSession

object SparkPersistence {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(SparkPersistence.getClass.getSimpleName)
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
