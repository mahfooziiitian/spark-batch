package com.mahfooz.spark.dataset.locals

import com.mahfooz.spark.dataset.model.Movie
import org.apache.spark.sql.SparkSession

object CreateDataSetLocal {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(CreateDataSetLocal.getClass.getSimpleName)
      .getOrCreate()

    val movies = spark.read.json("D:\\data\\movies\\movies.json")

    import spark.implicits._

    // convert DataFrame to strongly typed Dataset
    val moviesDS = movies.as[Movie]

    // create a Dataset using SparkSession.createDataset() and the toDS implicit function
    val localMovies = Seq(
      Movie("John Doe", "Awesome Movie", 2018L),
      Movie("Mary Jane", "Awesome Movie", 2018L)
    )

    val localMoviesDS1 = spark.createDataset(localMovies)

    localMoviesDS1.show

  }

}
