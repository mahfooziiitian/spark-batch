package com.mahfooz.spark.dataset.typed

import com.mahfooz.spark.dataset.model.Movie
import org.apache.spark.sql.SparkSession

object TypedDataset {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(TypedDataset.getClass.getSimpleName)
      .getOrCreate()

    val dataFilePath = sys.env.get("DATA_HOME")
      .map(data => data.replace("\\","/")).get
    val movies = spark.read.json(s"$dataFilePath/FileData/Json/Movies/movies.json")

    import spark.implicits._

    // convert DataFrame to strongly typed Dataset
    val moviesDS = movies.as[Movie]

    moviesDS.printSchema()

    //Manipulating a Dataset in a Type-Safe Manner
    // filter movies that were produced in 2010 using
    moviesDS.filter(movie => movie.produced_year == 2010).show(5)

    // displaying the title of the first movie in the moviesDS
    println(moviesDS.first.movie_title)

  }
}
