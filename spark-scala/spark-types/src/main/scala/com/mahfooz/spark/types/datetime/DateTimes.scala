package com.mahfooz.spark.types.datetime
import com.mahfooz.spark.types.numbers.std.ApproxStat.getClass
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, current_timestamp}

object DateTimes {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("DateTimes")
      .master("local")
      .getOrCreate()

    val dateDF = spark.range(10)  .withColumn("today", current_date())
      .withColumn("now", current_timestamp())

    dateDF.createOrReplaceTempView("dateTable")

    dateDF.printSchema()
  }
}
