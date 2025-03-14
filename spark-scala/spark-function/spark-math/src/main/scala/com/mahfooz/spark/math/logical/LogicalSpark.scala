/*

AND
  SELECT true and true;
    true
  SELECT true and false;
    false
  SELECT true and NULL;
    NULL
  SELECT false and NULL;
    false

 */
package com.mahfooz.spark.math.logical

import org.apache.spark.sql.SparkSession

object LogicalSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("LogicalSpark")
      .getOrCreate()

    spark.sql(" SELECT ! true").show()
    //expr1 and expr2 - Logical AND.

    spark.stop()
  }
}
