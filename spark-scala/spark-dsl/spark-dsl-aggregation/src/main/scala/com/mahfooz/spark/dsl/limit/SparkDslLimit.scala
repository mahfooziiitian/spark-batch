package com.mahfooz.spark.dsl.limit

import org.apache.spark.sql.SparkSession

object SparkDslLimit {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkDslLimit")
      .getOrCreate()

    val movies =
      spark.read.parquet(args(0))

    import spark.implicits._

    // first create a DataFrame with their name and associated length
    val actorNameDF = movies
      .select("actor_name")
      .distinct
      .selectExpr("*", "length(actor_name) as length")

    // order names by length and retrieve the top 10
    actorNameDF.orderBy('length.desc).limit(10).show(truncate = false)

    spark.stop()

  }
}
