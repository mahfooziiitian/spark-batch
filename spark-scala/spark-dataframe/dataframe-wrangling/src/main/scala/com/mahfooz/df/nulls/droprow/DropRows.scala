package com.mahfooz.df.nulls.droprow

import org.apache.spark.sql.{Row, SparkSession}

object DropRows {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DropRows")
      .config("spark.sql.warehouse.dir","D:\\data\\processing\\spark-warehouse")
      .getOrCreate()

    val badMovies = Seq(Row(null, null, null),
      Row(null, null, 2018L),
      Row("John Doe", "Awesome Movie", null),
      Row(null, "Awesome Movie", 2018L),
      Row("Mary Jane", null, 2018L))

    val movies=spark.read.json("D:\\data\\movies\\movies.json")

    val badMoviesRDD = spark.sparkContext.parallelize(badMovies)
    val badMoviesDF = spark.createDataFrame(badMoviesRDD, movies.schema)
    badMoviesDF.show

    // dropping rows that have missing data in any column
    // both of the lines below will achieve the same purpose
    badMoviesDF.na.drop().show
    badMoviesDF.na.drop("any").show

    // drop rows that have missing data in every single column
    badMoviesDF.na.drop("all").show

    // drops rows when column actor_name has missing data
    badMoviesDF.na.drop(Array("actor_name")).show
  }

}
