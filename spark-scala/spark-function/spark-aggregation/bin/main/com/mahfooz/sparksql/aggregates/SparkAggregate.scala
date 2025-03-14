/*

Applies a binary operator to an initial state and all elements in the array, and reduces this to a
single state.
The final state is converted into the final result by applying a finish function.

 */
package com.mahfooz.sparksql.aggregates

import org.apache.spark.sql.SparkSession

object SparkAggregate {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkAggregate")
      .getOrCreate()
    spark
      .sql(
        "SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x) as aggregate"
      )
      .show()
    spark.stop()
  }
}
