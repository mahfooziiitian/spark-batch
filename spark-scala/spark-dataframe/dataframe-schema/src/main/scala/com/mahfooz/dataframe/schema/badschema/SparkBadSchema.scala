package com.mahfooz.dataframe.schema.badschema

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object SparkBadSchema {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SparkBadSchema")
      .master("local[*]")
      .getOrCreate()

    val badMovieSchema = StructType(
      Array(
        StructField("actor_name", BooleanType, true),
        StructField("movie_title", StringType, true),
        StructField("produced_year", IntegerType, true)
      )
    )

    val movies = spark.read
      .schema(badMovieSchema)
      .json("D:\\data\\movies\\movies.json")

    movies.printSchema
    movies.show()
  }
}
