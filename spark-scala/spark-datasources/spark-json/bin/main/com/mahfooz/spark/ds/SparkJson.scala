/*

def json(jsonDataset: Dataset[String]): DataFrame
  Loads a Dataset[String] storing JSON objects (JSON Lines text format or newline-delimited JSON) and returns the result as a DataFrame.

def json(paths: String*): DataFrame
  Loads JSON files and returns the results as a DataFrame.

def json(path: String): DataFrame
  Loads a JSON file and returns the results as a DataFrame.

 */
package com.mahfooz.spark.ds

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkJson {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    //1. json method
    val json: DataFrame = spark.read
      .json(args(0))

    //2. format and load
    val json2: DataFrame = spark.read
      .format("json")
      .load(args(0))

    json.show()

    json2.show()

  }
}
