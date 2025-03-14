package com.mahfooz.spark.dataset.rdd

import org.apache.spark.sql.SparkSession

object RDD {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
        .appName("RDD")
        .master("Local")
        .getOrCreate()

    val rdd=spark.range(500).rdd

    spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))

    spark.range(10).rdd

  }
}
