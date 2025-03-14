package com.mahfooz.spark.dataframe.creating

import org.apache.spark.sql.SparkSession

object CreateDataFrameSequence {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("CreateDataFrameSequence")
      .master("local[*]")
      .getOrCreate()


    val movies = Seq(("Damon, Matt", "The Bourne Ultimatum", 2007L),
      ("Damon, Matt", "Good Will Hunting", 1997L))

    import spark.implicits._

    val moviesDF = movies.toDF("actor", "title", "year")

    moviesDF.printSchema

  }
}
