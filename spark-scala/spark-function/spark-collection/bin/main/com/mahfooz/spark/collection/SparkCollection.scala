/*



 */
package com.mahfooz.spark.collection

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkCollection {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkRegex")
      .getOrCreate()

    import spark.implicits._

    // create an tasks DataFrame
    val tasksDF = Seq(("Monday", Array("Pick Up John", "Buy Milk", "Pay Bill")))
      .toDF("day", "tasks")

    // schema of tasksDF
    tasksDF.printSchema

    // get the size of the array, sort it, and check to see if a particular value exists in the array
    tasksDF
      .select(
        'day,
        size('tasks).as("size"),
        sort_array('tasks).as("sorted_tasks"),
        array_contains('tasks, "Pay Bill").as("shouldPayBill")
      )
      .show(false)

  }
}
