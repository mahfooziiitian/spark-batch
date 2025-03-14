/*

There are many methods on RDDs that require you to put your data in a key–value format.
A hint that this is required is that the method will include <some-operation>ByKey.
Whenever you see ByKey in a method name, it means that you can perform this only on a PairRDD type.


 */
package com.mahfooz.spark.rdd.operation.transformation.wide.keyvalue

import org.apache.spark.sql.SparkSession

object KeyValue {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("KeyValue")
      .master("local")
      .getOrCreate()

    val rdd = spark.sparkContext.wholeTextFiles(
      "D:/data/processing/spark/spark-by-examples/txt/holmes.txt"
    )

    val words = rdd.flatMap(line => line._2.split(" "))

    words.map(word => (word.toLowerCase, 1))

  }
}
