/*

A SparkContext is the entry point for low-level API functionality.

You access it through the SparkSession, which is the tool you use to perform computation across a Spark cluster.

RDDs were the primary API in the Spark 1.X series and are still available in 2.X, but they are not as commonly
used.
An RDD represents an immutable, partitioned collection of records that can be operated on in parallel.
Unlike DataFrames though, where each record is a structured row containing fields with a known schema,
in RDDs the records are just Java, Scala, or Python objects of the programmer’s choosing.

Internally, each RDD is characterized by five main properties:

  A list of partitions

  A function for computing each split

  A list of dependencies on other RDDs

  Optionally, a Partitioner for key-value RDDs (e.g., to say that the RDD is hash-partitioned)

  Optionally, a list of preferred locations on which to compute each split (e.g., block locations for a Hadoop Distributed File System [HDFS] file)
There is no concept of “rows” in RDDs; individual records are just raw Java/Scala/Python objects, and you manipulate those
manually instead of tapping into the repository of functions that you have in the structured APIs.

 */
package com.mahfooz.spark.rdd

import org.apache.spark.sql.SparkSession

object Rdd {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    // in Scala: converts a Dataset[Long] to RDD[Long]
    spark.range(500).rdd

    //To operate on this data, you will need to convert this Row object to the correct data type
    // or extract values out of it.
    spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))

    //You can use the same methodology to create a DataFrame or Dataset from an RDD.
    import spark.implicits._
    spark.range(10).rdd.toDF()

  }
}
