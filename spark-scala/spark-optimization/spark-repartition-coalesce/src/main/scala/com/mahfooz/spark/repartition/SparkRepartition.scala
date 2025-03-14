package com.mahfooz.spark.repartition

import org.apache.spark.sql.SparkSession

object SparkRepartition {

  val dataHome = sys.env.getOrElse("DATA_HOME","data")

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkRepartition")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(Range(0,20))
    println("From local[*]"+rdd.partitions.size)

    val rdd1 = rdd.repartition(6)
    println("parallelize : "+rdd1.partitions.size)

    val rddFromFile = spark.sparkContext.textFile(s"${dataHome}/spark/datasource/text/test.txt",10)
    println("TextFile : "+rddFromFile.partitions.size)

    rdd1.saveAsTextFile(s"${dataHome}/spark/datasource/text/partition")

    val rdd2 = rdd1.repartition(4)
    println("Repartition size : "+rdd2.partitions.size)

    rdd2.saveAsTextFile(s"${dataHome}/spark/datasource/text/re-partition")

  }

}
