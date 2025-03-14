package com.mahfooz.spark.types.numbers.std

import com.mahfooz.spark.types.numbers.SparkNumbers.getClass
import org.apache.spark.sql.SparkSession

object DescribeStat {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("DescribeStat")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    df.describe().show()

  }
}
