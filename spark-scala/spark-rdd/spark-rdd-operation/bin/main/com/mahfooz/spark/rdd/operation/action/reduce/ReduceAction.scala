/*
Action returns values to the driver program.
reduce(func) returns a data set by aggregating the elements of the data set using a function func.
The function takes two arguments and returns a single argument.
The function should be commutative and associative so that it can be operated in parallel.

The word commutative comes from commute or move around, so the commutative property is the one that refers to moving stuff around.
For addition, the rule is a + b = b + a

The word associative comes from associate or group; the associative property is the rule that refers to grouping.

For addition, the rule is a + (b + c) = (a + b) +c

 */
package com.mahfooz.spark.rdd.operation.action.reduce

import org.apache.spark.sql.SparkSession

object ReduceAction {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ReduceAction")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val count = sc
      .parallelize(1 to 20)
      .reduce(_ + _)
    println(count)
  }
}
