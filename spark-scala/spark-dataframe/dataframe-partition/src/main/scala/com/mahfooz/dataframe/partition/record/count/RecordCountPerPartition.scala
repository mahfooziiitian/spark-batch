package com.mahfooz.dataframe.partition.record.count

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.spark_partition_id

object RecordCountPerPartition {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("records-count-per-partition")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.range(10000).repartition(20)

    val countPerPartition = df.withColumn("partition_id", spark_partition_id)
      .groupBy("partition_id").count

    countPerPartition.show(false)

    df
      .rdd
      .mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
      .toDF("partition_number","number_of_records")
      .show
  }

}
