/*

NewHadoopRDD provides core functionality for reading data stored in HDFS, HBase tables, Amazon S3
using the new MapReduce API from Hadoop 2.x libraries.
NewHadoopRDD can read from many different formats thus is used to interact with several external
systems.

class NewHadoopRDD[K, V](
 sc : SparkContext,
 inputFormatClass: Class[_ <: InputFormat[K, V]],
 keyClass: Class[K],
 valueClass: Class[V],
 @transient private val _conf: Configuration)
extends RDD[(K, V)]

SparkContext's wholeTextFiles function to create WholeTextFileRDD.
Now, WholeTextFileRDD actually extends NewHadoopRDD.

 */
package com.mahfooz.spark.rdd.types.hadoop

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.spark.sql.SparkSession

object NewHadoopRDDEx {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("NewHadoopRDDEx")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd_whole = sc.wholeTextFiles("D:\\data\\spark\\text\\spark_test.txt")
    print(rdd_whole.toDebugString)

    val newHadoopRDD = sc.newAPIHadoopFile("D:\\data\\spark\\csv\\statesPopulation.csv",
      classOf[KeyValueTextInputFormat],
      classOf[Text],classOf[Text])

    newHadoopRDD.foreach(record=>println(record))

  }
}
