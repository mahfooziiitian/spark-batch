/*

Pair RDDs are RDDs consisting of key-value tuples which suits many use cases such as aggregation,
sorting, and joining data.
The keys and values can be simple types such as integers and strings or more complex types such as
case classes, arrays, lists, and other types of collections.
The key-value based extensible data model offers many advantages and is the fundamental concept behind the MapReduce paradigm
 */
package com.mahfooz.spark.rdd.types.pair

import org.apache.spark.sql.SparkSession

object PairRDD {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PairRDD")
      .getOrCreate()

    val sc = spark.sparkContext
    val dataPath =
      s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/Csv/statesPopulation.csv"
    val statesPopulationRDD =
      sc.textFile(dataPath)
    println(statesPopulationRDD.first)

    statesPopulationRDD.take(5).foreach(record => println(record))

    val pairRDD = statesPopulationRDD.map(record =>
      (record.split(",")(0), record.split(",")(2))
    )

    pairRDD.foreach(record=>println(record._1+","+record._2))

  }
}
