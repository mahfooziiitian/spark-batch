package com.mahfooz.spark.df.ds.csv

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object TscFileReader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val movieSchema = StructType(Array(StructField("actor_name", StringType, true),
      StructField("movie_title", StringType, true),
      StructField("produced_year", LongType, true)))

    val movies = spark.read
      .option("header", "true")
      .option("sep", "\t")
      .schema(movieSchema)
      .csv("D:\\data\\movies\\movies.tsv")

    movies.printSchema

    movies.show()
  }

}
