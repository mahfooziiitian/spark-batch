package com.mahfooz.spark.dsl.union

import org.apache.spark.sql.{Row, SparkSession}

object SparkUnionDsl {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkUnionDsl")
      .getOrCreate()

    val movies =
      spark.read.parquet(args(0))

    import spark.implicits._

    val shortNameMovieDF = movies.where('movie_title === "12")
    shortNameMovieDF.show

    val forgottenActor = Seq(Row("Brychta, Edita", "12", 2007L))
    val forgottenActorRDD = spark.sparkContext.parallelize(forgottenActor)
    val forgottenActorDF =
      spark.createDataFrame(forgottenActorRDD, shortNameMovieDF.schema)

    // now adding the missing action
    val completeShortNameMovieDF = shortNameMovieDF.union(forgottenActorDF)
    completeShortNameMovieDF.union(forgottenActorDF).show

    spark.stop()

  }
}
