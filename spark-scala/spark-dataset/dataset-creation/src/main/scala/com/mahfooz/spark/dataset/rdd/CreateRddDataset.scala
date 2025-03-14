package com.mahfooz.spark.dataset.rdd

import com.mahfooz.spark.dataset.model.Movie
import org.apache.spark.sql.SparkSession

object CreateRddDataset {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(CreateRddDataset.getClass.getSimpleName)
      .getOrCreate()

    val seqMovies = Seq(
      Movie("John Doe", "Awesome Movie", 2018L),
      Movie("Mary Jane", "Awesome Movie", 2018L)
    )

    val rdd = spark.sparkContext.parallelize(seqMovies)
    import spark.implicits._
    val rddToDs = rdd.toDS()

    rddToDs.show

  }
}
