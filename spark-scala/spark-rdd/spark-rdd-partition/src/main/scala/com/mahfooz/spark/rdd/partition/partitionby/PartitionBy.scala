package com.mahfooz.spark.rdd.partition.partitionby

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PartitionBy {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("PartitionBy")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val dataPath = s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/"

    val rdd = sc.textFile(s"${dataPath}/Csv/zipcodes.csv")
    val rdd2: RDD[Array[String]] = rdd.map(m => m.split(","))
    val rdd3 = rdd2.map(a => (a(1), a.mkString(",")))
    val rdd4 = rdd3.partitionBy(new HashPartitioner(3))

    rdd4.saveAsTextFile(s"${dataPath}/Csv/zipcodePartitioned")

  }
}
