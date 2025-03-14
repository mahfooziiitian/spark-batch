/*

spark.read.orc("<path>")
spark.read.format("orc")

Key         Values                Default       Description

sep         Single character      ,             This is a single-character value used as a delimiter for each column.
header      true, false           false         If the value is true, it means the first line in the file represents the column names.
escape      Any character         \             This is the character to use to escape the character in the column value that is the same as sep.
inferSchema true, false           false         This specifies whether Spark should try to infer the column type based on column value.


def csv(paths: String*): DataFrame
  Loads CSV files and returns the result as a DataFrame.

def csv(csvDataset: Dataset[String]): DataFrame
  Loads an Dataset[String] storing CSV rows and returns the result as a DataFrame.

def csv(path: String): DataFrame
  Loads a CSV file and returns the result as a DataFrame.

  D:\\data\\movies\\moveis.csv
 */
package com.mahfooz.spark.csv

import org.apache.spark.sql.{DataFrame, SparkSession}

object CSVReaders {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val csvs: DataFrame = spark.read
      .format("csv")
      .option("sep", ",")
      .option("escape", ",")
      .option("header", true)
      .option("inferSchema", true)
      .load(args(0))

    csvs.show()

    val movies = spark.read
      .option("header", "true")
      .csv(args(0))

    movies.show()
    movies.printSchema

    val movies2 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    movies2.printSchema

    import org.apache.spark.sql.types._

    val movieSchema = StructType(
      Array(
        StructField("actor_name", StringType, true),
        StructField("movie_title", StringType, true),
        StructField("produced_year", LongType, true)
      )
    )

    val movies3 = spark.read
      .option("header", "true")
      .schema(movieSchema)
      .csv(args(0))

    movies3.show()
    movies3.printSchema()
  }
}
