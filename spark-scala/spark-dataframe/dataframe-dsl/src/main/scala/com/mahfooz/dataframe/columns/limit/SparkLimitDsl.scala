package com.mahfooz.dataframe.columns.limit

import org.apache.spark.sql.SparkSession

object SparkLimitDsl {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(SparkLimitDsl.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val movies = spark.read.json("D:\\data\\movies\\movies.json")

    // first create a DataFrame with their name and associated length
    val actorNameDF = movies.select("actor_name")
      .distinct
      .selectExpr("*", "length(actor_name) as length")

    import spark.implicits._

    // order names by length and retrieve the top 10
    actorNameDF.orderBy('length.desc).limit(10).show

  }

}
