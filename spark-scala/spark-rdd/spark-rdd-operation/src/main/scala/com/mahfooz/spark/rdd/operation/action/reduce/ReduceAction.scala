/*
Action returns values to the driver program.

reduce(func) returns a data set by aggregating the elements of the data set using a function func.

1. The function takes two arguments and returns a single argument.
2. The function should be commutative and associative so that it can be operated in parallel.

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
