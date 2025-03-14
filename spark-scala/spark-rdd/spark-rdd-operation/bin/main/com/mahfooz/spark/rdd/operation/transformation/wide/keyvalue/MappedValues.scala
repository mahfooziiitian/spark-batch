package com.mahfooz.spark.rdd.operation.transformation.wide.keyvalue

import org.apache.spark.sql.SparkSession

object MappedValues {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(MappedValues.getClass.getName)
      .master("local")
      .getOrCreate()

    val rdd = spark.sparkContext.wholeTextFiles(
      "D:/data/processing/spark/spark-by-examples/txt/holmes.txt"
    )

    val words = rdd
      .filter(line => !line._2.isEmpty)
      .flatMap(line => line._2.split(" "))

    val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)

    keyword.mapValues(word => word.toUpperCase).collect()

    keyword.flatMapValues(word => word.toUpperCase).collect()

    keyword.keys.collect()

    keyword.values.collect()
  }
}
