package com.mahfooz.spark.dsl.select

import org.apache.spark.sql.SparkSession

object SparkSelectDsl {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val movies =
      spark.read.parquet(args(0))

    movies.select("movie_title", "produced_year").show(5)

    import spark.implicits._

    movies
      .select(
        'movie_title,
        ('produced_year - ('produced_year % 10)).as("produced_decade")
      )
      .show(5)

    spark.stop()
  }

}
