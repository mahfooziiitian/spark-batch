package com.mahfooz.dataframe.columns.union

import org.apache.spark.sql.{Row, SparkSession}

object UnionDf {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .appName(UnionDf.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val movies = spark.read.json("D:\\data\\movies\\movies.json")

    import spark.implicits._

    // we want to add a missing actor to movie with title as "12"
    val shortNameMovieDF = movies.where('movie_title === "12")
    shortNameMovieDF.show

    val forgottenActor = Seq(Row("Brychta, Edita", "12", 2007L))
    val forgottenActorRDD = spark.sparkContext.parallelize(forgottenActor)
    val forgottenActorDF = spark.createDataFrame(forgottenActorRDD, shortNameMovieDF.schema)

    // now adding the missing action
    val completeShortNameMovieDF = shortNameMovieDF.union(forgottenActorDF)

    completeShortNameMovieDF.show

  }
}
