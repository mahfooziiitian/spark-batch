package com.mahfooz.spark.dataset.dataframe

import org.apache.spark.sql.SparkSession

// define Movie case class
case class Movie(actor_name: String, movie_title: String, produced_year: Long)

object DsToDf {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DsToDf")
      .getOrCreate()

    val movies =
      spark.read.parquet(args(0))

    import spark.implicits._

    // convert DataFrame to strongly typed Dataset
    val moviesDS = movies.as[Movie]

    moviesDS.show()

    val localMovies = Seq(
      Movie("John Doe", "Awesome Movie", 2018L),
      Movie("Mary Jane", "Awesome Movie", 2018L)
    )

    val localMoviesDS = spark.createDataset(localMovies)

    val localMoviesDS2 = localMovies.toDS()
    localMoviesDS.show

    moviesDS.filter(movie => movie.produced_year == 2010).show(5)

    // filter movies that were produced in 2010 using
    moviesDS.filter(movie => movie.produced_year == 2010).show(5)

    // displaying the title of the first movie in the moviesDS
    moviesDS.first.movie_title

    // perform projection using map transformation
    val titleYearDS = moviesDS.map(m => (m.movie_title, m.produced_year))
    titleYearDS.printSchema

    // demonstrating a type-safe transformation that fails at compile time, performing subtraction on a column with string type
    // a problem is not detected for DataFrame until runtime
    movies.select('movie_title - 'movie_title)

    // take action returns rows as Movie objects to the driver
    moviesDS.take(5).foreach(println)

    spark.stop()

  }
}
