package com.mahfooz.spark.df.ds.json.schema

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SparkBadSchemaJson {

  def main(args: Array[String]): Unit = {

    val PATH_PREFIX = "D:\\data\\movies"

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val badMovieSchema = StructType(Array(StructField("actor_name", BooleanType, true),
      StructField("movie_title", StringType, true),
      StructField("produced_year", IntegerType, true)))

    val movies7 = spark.read
      .schema(badMovieSchema)
      .json(PATH_PREFIX + "\\movies.json")

    movies7.printSchema

    movies7.show(5)

    val movies8 = spark.read.option("mode", "failFast")
      .schema(badMovieSchema)
      .json(PATH_PREFIX + "\\movies.json")

    movies8.printSchema

    movies8.show(5)
  }

}
