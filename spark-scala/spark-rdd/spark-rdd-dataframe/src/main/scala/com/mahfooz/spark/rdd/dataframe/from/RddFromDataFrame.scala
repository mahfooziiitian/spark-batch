/*
You will notice that if you do a conversion from a Dataset[T] to an RDD, you’ll get the appropriate native type T back
(remember this applies only to Scala and Java).

 */
package com.mahfooz.spark.rdd.dataframe.from

import org.apache.spark.sql.SparkSession

object RddFromDataFrame {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("RddFromDataFrame")
      .getOrCreate()

    //Dataset[Long] to RDD[Long]
    val rdd=spark.range(500).toDF().rdd

    rdd.map(rawObject=>rawObject.getLong(0))

  }

}
