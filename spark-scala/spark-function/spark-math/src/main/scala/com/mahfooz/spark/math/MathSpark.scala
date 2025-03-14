/*


 */
package com.mahfooz.spark.math

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MathSpark {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("MathSpark")
      .getOrCreate()

    import spark.implicits._

    val numberDF = Seq(
      ("3.124", "65.345346", "2017")
    ).toDF("pie", "gpa", "year")

    numberDF
      .select(
        round('pie).as("pie0"),
        round('pie, 1).as("pie1"),
        round('pie, 2).as("pie2"),
        round('gpa).as("gpa"),
        round('year).as("year")
      )
      .show

  }
}
