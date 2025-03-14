package com.mahfooz.sparksql.generic.manual

import org.apache.spark.sql.SparkSession

object ManualSaveLoad {

  def main(args: Array[String]): Unit = {

    val inputFile=args(0)
    val outputFile=args(1)

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val peopleDF = spark
      .read
      .format("json")
      .load(inputFile)

    peopleDF.printSchema()

    peopleDF
      .select("name", "age")
      .write
      .format("parquet")
      .save(outputFile)

  }
}
