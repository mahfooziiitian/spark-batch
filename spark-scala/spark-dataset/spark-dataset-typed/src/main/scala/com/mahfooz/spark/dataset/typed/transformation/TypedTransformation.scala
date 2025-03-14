package com.mahfooz.spark.dataset.typed.transformation

import com.mahfooz.spark.dataset.model.Movie
import org.apache.spark.sql.SparkSession

object TypedTransformation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(TypedTransformation.getClass.getSimpleName)
      .getOrCreate()

    val movies = spark.read.json("D:/data/movies/movies.json")

    import spark.implicits._

    // convert DataFrame to strongly typed Dataset
    val moviesDS = movies.as[Movie]

    // perform projection using map transformation
    val titleYearDS = moviesDS.map(m => ( m.movie_title, m.produced_year))

    titleYearDS.printSchema

    spark.stop()

  }
}
