package com.mahfooz.spark.dataset.collection

import com.mahfooz.spark.dataset.model.Movie
import org.apache.spark.sql.SparkSession

object CreateDatasetFromSeq {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse =
      sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse").replace("\\", "/")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CreateDatasetFromSeq")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val localMovies = Seq(
      Movie("John Doe", "Awesome Movie", 2018L),
      Movie("Mary Jane", "Awesome Movie", 2018L)
    )

    import spark.implicits._
    val localMoviesDS = localMovies.toDS()

    localMoviesDS.show

  }
}
