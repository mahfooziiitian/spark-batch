package com.mahfooz.spark.collection.explode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object SparkExplode {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkExplode")
      .getOrCreate()

    import spark.implicits._

    // create an tasks DataFrame
    val tasksDF = Seq(("Monday", Array("Pick Up John", "Buy Milk", "Pay Bill")))
      .toDF("day", "tasks")

    // the explode function will create a new row for each element in the array
    tasksDF.select('day, explode('tasks)).show

  }
}
