/*

 */
package com.mahfooz.spark.ds.config

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  BooleanType,
  IntegerType,
  StringType,
  StructField,
  StructType
}

object SparkJsonOption {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val movieSchema = StructType(
      Array(
        StructField("actor_name", StringType, true),
        StructField("movie_title", StringType, true),
        StructField("produced_year", IntegerType, true)
      )
    )

    val movies = spark.read
      .option("inferSchema", "true")
      .schema(movieSchema)
      .json(args(0))

    movies.printSchema
    movies.show()

    val badMovieSchema = StructType(
      Array(
        StructField("actor_name", BooleanType, true),
        StructField("movie_title", StringType, true),
        StructField("produced_year", IntegerType, true)
      )
    )

    val movies2 = spark.read
    //.option("mode", "failFast")
      .schema(badMovieSchema)
      .json(args(0))

    movies2.printSchema
    movies2.show()

  }
}
