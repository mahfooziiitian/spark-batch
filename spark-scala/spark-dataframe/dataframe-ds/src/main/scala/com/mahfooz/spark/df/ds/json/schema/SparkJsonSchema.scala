package com.mahfooz.spark.df.ds.json.schema

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkJsonSchema {

  def main(args: Array[String]): Unit = {

    val PATH_PREFIX="D:\\data\\movies"

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val movieSchema2 = StructType(Array(StructField("actor_name", StringType, true),
      StructField("movie_title", StringType, true),
      StructField("produced_year", IntegerType, true)))

    val movies6 = spark.read
      .option("inferSchema","true")
      .schema(movieSchema2)
      .json(PATH_PREFIX+"\\movies.json")

    movies6.printSchema

  }

}
