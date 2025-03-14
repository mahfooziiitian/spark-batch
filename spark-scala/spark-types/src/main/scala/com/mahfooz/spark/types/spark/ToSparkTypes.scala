package com.mahfooz.spark.types.spark
import com.mahfooz.spark.types.SparkTypes.getClass
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object ToSparkTypes {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("ToSparkTypes")
      .master("local")
      .getOrCreate()

    // in Scala
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    df.select(lit(5), lit("five"), lit(5.0))
  }
}
