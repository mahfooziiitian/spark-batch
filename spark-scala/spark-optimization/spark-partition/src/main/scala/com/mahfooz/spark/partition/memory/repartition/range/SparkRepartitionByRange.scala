package com.mahfooz.spark.partition.memory.repartition.range

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkRepartitionByRange {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("spark-repartition-by-range")
      .master("local[*]")
      .getOrCreate()

    val driverData = Seq(
      ("Alice", "A224455", "Female", 3000),
      ("Bryan","B992244","Male",4000),
      ("Catherine","C887733","Female",2000),
      ("Daryl","D229988","Male",3000),
      ("Jenny","J663300","Female", 6000))

    import spark.implicits._

    val df = driverData.toDF("name","license","gender","salary")
    println("Default Partitions: " + df.rdd.getNumPartitions)

    val repartitionRangeDF = df.repartitionByRange(1,
      col("salary"))
    println("Range Partitions: " + repartitionRangeDF.rdd.getNumPartitions)
  }

}
