package com.mahfooz.sparksql.schema.json

import org.apache.spark.sql.SparkSession

object JsonFileLoad {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("JsonFileLoad")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("multiLine", value = true)
      .option("mode", "PERMISSIVE")
      .json(args(0))

    df.show(truncate=false)
    df.printSchema()
    println(df.schema.prettyJson)
  }
}
