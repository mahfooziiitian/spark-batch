package com.mahfooz.spark.strings.reverse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ReverseSpark {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ReverseSpark")
      .getOrCreate()

    import spark.implicits._

    // transform a string with concatenation, uppercase, lowercase and reverse
    val sparkAwesomeDF =
      Seq(("Spark", "is", "awesome")).toDF("subject", "verb", "adj")

    sparkAwesomeDF
      .select(concat_ws(" ", 'subject, 'verb, 'adj).as("sentence"))
      .select(
        lower('sentence).as("lower"),
        upper('sentence).as("upper"),
        initcap('sentence).as("initcap"),
        reverse('sentence).as("reverse")
      )
      .show

  }
}
