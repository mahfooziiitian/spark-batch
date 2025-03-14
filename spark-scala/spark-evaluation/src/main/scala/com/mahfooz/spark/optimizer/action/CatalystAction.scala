package com.mahfooz.spark.optimizer.action

import org.apache.spark.sql.SparkSession

object CatalystAction {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PhysicalPlan")
      .getOrCreate()

    import spark.implicits._

    // read movies data in Parquet format
    val moviesDF = spark.read.load("d:/data/movies/movies.parquet")

    // perform two filtering conditions
    val newMoviesDF = moviesDF.filter('produced_year > 1970)
      .withColumn("produced_decade", 'produced_year + 'produced_year % 10)
      .select('movie_title, 'produced_decade).where('produced_decade > 2010)

    // display the logical and physical plans
    newMoviesDF.explain(true)
  }
}
