package com.mahfooz.spark.codec.decoder

import org.apache.spark.sql.SparkSession

object DecoderPlan {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DecoderPlan")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    import spark.implicits._

    val dataset = spark.range(5).withColumn("group", 'id % 2)
    println(s"num_partition = ${dataset.rdd.getNumPartitions}")
    println(dataset.rdd.toDebugString)

    println(dataset.queryExecution.toRdd.toDebugString)
    println(s"num_partition = ${dataset.queryExecution.toRdd.getNumPartitions}")

  }

}
