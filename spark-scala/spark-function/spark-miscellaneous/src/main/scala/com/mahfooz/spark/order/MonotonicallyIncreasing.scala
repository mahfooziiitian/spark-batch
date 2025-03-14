package com.mahfooz.spark.order

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{monotonically_increasing_id, spark_partition_id}

object MonotonicallyIncreasing {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("MonotonicallyIncreasing")
      .getOrCreate()

    import spark.implicits._

    val numDF = spark.range(1, 11, 1, 5)

    // now generate the monotonically increasing numbers and see which ones are in which partition
    numDF
      .select(
        'id,
        monotonically_increasing_id().as("m_ii"),
        spark_partition_id().as("partition")
      )
      .show

  }

}
