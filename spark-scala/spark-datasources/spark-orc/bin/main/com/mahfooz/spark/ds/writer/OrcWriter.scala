package com.mahfooz.spark.ds.writer

import org.apache.spark.sql.SparkSession

object OrcWriter {

  def main(args: Array[String]): Unit ={

    val spark=SparkSession.builder
      .appName("")
      .master("local[*]")
      .getOrCreate()

    val usersDF = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("D:\\data\\spark\\csv\\people.csv")

    usersDF.write.format("orc")
      .option("orc.bloom.filter.columns", "favorite_color")
      .option("orc.dictionary.key.threshold", "1.0")
      .option("orc.column.encoding.direct", "name")
      .save("users_with_options.orc")

    spark.stop()
  }

}
