/*
Accumulators are shared variables across executors typically used to add counters to your Spark program.
If you have a Spark program and would like to know errors or total records processed or both, you can do it
in two ways. One way is to add extra logic to just count errors or total records, which becomes complicated
when handling all possible computations.
The other way is to leave the logic and code flow fairly intact and add Accumulators.
Accumulators can only be updated by adding to the value.

 */
package com.mahfooz.spark.rdd.accumulator

import org.apache.spark.sql.SparkSession

object RDDAccumulator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(RDDAccumulator.getClass.getName)
      .master("local")
      .getOrCreate()

    val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")

    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3))

    rdd.foreach(x => longAcc.add(x))
    println(longAcc.value)
  }

}
