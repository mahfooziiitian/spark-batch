package com.mahfoz.elk.search.read

import org.apache.spark.sql.SparkSession

object ReadFromES {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val es_df = spark.read.format("org.elasticsearch.spark.sql")
      .load("simpleindex")
    println(es_df.count())
  }
}
