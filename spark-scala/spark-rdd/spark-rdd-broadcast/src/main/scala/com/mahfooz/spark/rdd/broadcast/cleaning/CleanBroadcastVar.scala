/*

Broadcast variables do occupy memory on all executors and depending on the size of the data contained in the
broadcast variable, this could cause resource issues at some point.
There is a way to remove broadcast variables from the memory of all executors.

Calling unpersist() on a broadcast variable removed the data of the broadcast variable from the memory cache
of all executors to free up resources.
If the variable is used again, then the data is retransmitted to the executors in order for it to be used again.
The Driver, however, holds onto the memory as if the Driver does not have the data, then broadcast variable is
no longer valid.
 */
package com.mahfooz.spark.rdd.broadcast.cleaning

import org.apache.spark.sql.SparkSession

object CleanBroadcastVar {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(CleanBroadcastVar.getClass.getName)
      .master("local[3]")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd_one = sc.parallelize(Seq(1, 2, 3))
    val k = 5
    val bk = sc.broadcast(k)
    rdd_one.map(j => j + bk.value).take(5)
    bk.unpersist
    rdd_one.map(j => j + bk.value).take(5)
    bk.destroy
  }
}
