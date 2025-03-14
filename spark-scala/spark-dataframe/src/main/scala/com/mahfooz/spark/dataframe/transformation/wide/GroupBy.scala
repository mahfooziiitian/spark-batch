package com.mahfooz.spark.dataframe.transformation.wide

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object GroupBy {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "5")

    import spark.implicits._

    val flightData2015 = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(args(0))

    flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .show()
  }
}
