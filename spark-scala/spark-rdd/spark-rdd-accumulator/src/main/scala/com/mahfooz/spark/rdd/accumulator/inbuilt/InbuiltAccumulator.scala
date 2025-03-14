/*

Each of these accumulator classes has several methods, among these, add() method call from tasks running on the cluster.
Tasks can’t read the values from the accumulator and only the driver program can read accumulators value using the value()
method.

longAccumulator() methods from SparkContext returns LongAccumulator

LongAccumulator class provides following methods

  isZero
  copy
  reset
  add
  count
  sum
  avg
  merge
  value

 */
package com.mahfooz.spark.rdd.accumulator.inbuilt

import org.apache.spark.sql.SparkSession

object InbuiltAccumulator {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("InbuiltAccumulator")
      .getOrCreate()

    val sc=spark.sparkContext

    val accumulator = sc.longAccumulator("SumAccumulator")

    sc.parallelize(Array(1, 2, 3)).foreach(x => accumulator.add(x))

    //Only driver can access accumulator.value
    println(accumulator.value)

    println( accumulator.count)
    println(accumulator.sum)
    println(accumulator.avg)
    println(accumulator.id)
    accumulator.reset()
    println(accumulator.isZero)
  }
}
