/*

 */
package com.mahfooz.spark.json.config

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}

object SparkJsonOption {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .config("spark.sql.warehouse.dir",sparkWarehouse)
      .getOrCreate()

    val movieSchema = StructType(
      Array(
        StructField("actor_name", StringType, true),
        StructField("movie_title", StringType, true),
        StructField("produced_year", IntegerType, true)
      )
    )

    val dataHome = sys.env.getOrElse("DATA_HOME","data")
    val csvDataPath = s"${dataHome}/FileData/Csv/Movies/movies.csv"
    val jsonDataPath = s"${dataHome}/FileData/Json/Movies/movies.json"

    spark.read
      .option("mode", "FAILFAST")
      .option("header", "true")
      .schema(movieSchema)
      .csv(csvDataPath)
      .write.format("json")
      .mode(SaveMode.Ignore)
      .save(jsonDataPath)

    val movies = spark.read
      .option("inferSchema", "true")
      .option("mode","FAILFAST")
      .schema(movieSchema)
      .json(jsonDataPath)

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
      .json(jsonDataPath)

    movies2.printSchema
    movies2.show()

  }
}
