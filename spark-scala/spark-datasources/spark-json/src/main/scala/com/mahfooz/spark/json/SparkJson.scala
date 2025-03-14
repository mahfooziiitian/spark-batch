/*

def json(jsonDataset: Dataset[String]): DataFrame
  Loads a Dataset[String] storing JSON objects (JSON Lines text format or newline-delimited JSON) and returns the result as a DataFrame.

def json(paths: String*): DataFrame
  Loads JSON files and returns the results as a DataFrame.

def json(path: String): DataFrame
  Loads a JSON file and returns the results as a DataFrame.

 */
package com.mahfooz.spark.json

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkJson {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .config("spark.sql.warehouse.dir",sparkWarehouse)
      .getOrCreate()

    val dataHome = sys.env.getOrElse("DATA_HOME","data")

    //1. json method
    val json: DataFrame = spark.read
      .json(s"${dataHome}/FileData/Json/2015-summary.json")

    json.show()

    //2. format and load
    val json2: DataFrame = spark.read
      .format("json")
      .load(s"${dataHome}/FileData/Json/2015-summary.json")

    json2.show()

  }
}
