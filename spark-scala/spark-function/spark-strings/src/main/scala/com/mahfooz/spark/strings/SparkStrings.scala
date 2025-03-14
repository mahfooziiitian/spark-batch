package com.mahfooz.spark.strings

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkStrings {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("MathSpark")
      .getOrCreate()

    import spark.implicits._

    val sparkDF = Seq(("  Spark  ")).toDF("name")

    // trimming
    sparkDF
      .select(
        trim('name).as("trim"),
        ltrim('name).as("ltrim"),
        rtrim('name).as("rtrim")
      )
      .show

    sparkDF
      .select(trim('name).as("trim"))
      .select(lpad('trim, 8, "-").as("lpad"), rpad('trim, 8, "=").as("rpad"))
      .show

  }
}
