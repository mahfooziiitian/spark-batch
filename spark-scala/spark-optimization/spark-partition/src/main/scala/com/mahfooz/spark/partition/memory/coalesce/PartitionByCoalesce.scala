package com.mahfooz.spark.partition.memory.coalesce

import org.apache.spark.sql.SparkSession

object PartitionByCoalesce {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse")

    val spark = SparkSession
      .builder()
      .appName("PartitionByCoalesce")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
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

    val coalesceDF=df.coalesce(2)
    println("Coalesce Partitions: " + coalesceDF.rdd.getNumPartitions)

  }

}
