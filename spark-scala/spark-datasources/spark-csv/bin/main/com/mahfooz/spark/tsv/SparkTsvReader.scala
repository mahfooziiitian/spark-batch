package com.mahfooz.spark.tsv

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType
}

object SparkTsvReader {

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
        StructField("produced_year", LongType, true)
      )
    )

    val movies = spark.read
      .option("header", "true")
      .option("sep", "\t")
      .schema(movieSchema)
      .csv(args(0))

    movies.printSchema
    movies.show(5)
  }
}
