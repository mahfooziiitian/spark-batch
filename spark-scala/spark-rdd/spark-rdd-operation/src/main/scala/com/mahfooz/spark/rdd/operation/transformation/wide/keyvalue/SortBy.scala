/*

Sorts the rows according to the keys.

By default, the keys are sorted in ascending order.

This transformation is simple to understand.

It sorts the rows according the key, and there is an option to specify whether the result should be in ascending
(default) or descending order.

 */
package com.mahfooz.spark.rdd.operation.transformation.wide.keyvalue

import com.mahfooz.spark.rdd.model.ZipCode
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SortBy {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName(SortBy.getClass.getName)
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd:RDD[String] = sc.textFile(getClass.getResource("/zipcodes-noheader.csv").getFile)

    val rddZip:RDD[ZipCode] = rdd.map(f=>{
      val arr = split(f)
      ZipCode(arr(0).toInt,arr(1),arr(3),arr(4))
    })

    //SortBy
    val rddSort = rddZip.sortBy(f=>f.recordNumber)
    rddSort.collect().foreach(f=>println(f.toString))

    //SorybyKey
    //First create pairRDD
    val rddTuple=rddZip.map(f=>{
      Tuple2(f.recordNumber,f.toString)
    })
    rddTuple.sortByKey().collect().foreach(f=>println(f._2))
  }

  def split(str:String): Array[String] ={
    str.split(",")
  }

}
