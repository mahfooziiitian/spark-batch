package com.mahfooz.spark.partition.memory.repartition

import org.apache.spark.sql.SparkSession

object SparkRepartitionMemory {
  
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("spark-repartition-memory")
      .master("local[*]")
      .getOrCreate()

    val driverData = Seq(
      ("Alice", "A224455", "Female", 3000),
      ("Bryan", "B992244", "Male", 4000),
      ("Catherine", "C887733", "Female", 2000),
      ("Daryl", "D229988", "Male", 3000),
      ("Jenny", "J663300", "Female", 6000))

    import spark.implicits._

    val df = driverData.toDF
    println("Default Partitions: " + df.rdd.getNumPartitions)

    val repartitionDF = df.repartition(3)
    println("Repartition Partitions: " + repartitionDF.rdd.getNumPartitions)
  }
}
