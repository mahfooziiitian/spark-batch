/*

SELECT *, 1 as One FROM dfTable LIMIT 2

 */
package com.mahfooz.dataframe.transformation.projection

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, lit}

object ProjectLiteralAsColumn {

  private def start(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("ProjectLiteralAsColumn")
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val spark = start()
    val df = spark.read.json("D:\\data\\flight-data\\json\\2015-summary.json")
    df.select(expr("*"), lit(1).as("One")).show(2)
  }

}
