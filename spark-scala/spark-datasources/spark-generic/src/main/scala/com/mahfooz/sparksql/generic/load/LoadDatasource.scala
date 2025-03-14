/*
def load(paths: String*): DataFrame
  Loads input in as a DataFrame, for data sources that support multiple paths.

def load(path: String): DataFrame
  Loads input in as a DataFrame, for data sources that require a path.

def load(): DataFrame
  Loads input in as a DataFrame, for data sources that don't require a path.

 */
package com.mahfooz.sparksql.generic.load

import org.apache.spark.sql.SparkSession

object LoadDatasource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    var movies =
      spark.read.load(args(0))
    movies.printSchema

    // If we want to be more explicit, we can specify the path to the parquet function
    movies = spark.read.parquet(args(0))

    movies.printSchema

  }
}
