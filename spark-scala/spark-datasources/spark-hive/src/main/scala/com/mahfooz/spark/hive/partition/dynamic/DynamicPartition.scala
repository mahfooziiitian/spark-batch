package com.mahfooz.spark.hive.partition.dynamic

import com.mahfooz.spark.hive.common.SparkHiveCommon
import org.apache.spark.sql.SaveMode

object DynamicPartition {

  def main(args: Array[String]): Unit = {

    val stagingTableName=args(0)
    val partitionTable=args(1)
    val partitionKey=args(2)

    val spark = SparkHiveCommon.createSparkDynamicSession("DynamicPartition")

    import spark.sql

    // Read data from the Hive managed table into dataframe
    val df = spark.table(stagingTableName)

    // Create a Hive partitioned table using DataFrame API
    df.write
      .partitionBy(partitionKey)
      .mode(SaveMode.Append)
      .format("hive")
      .saveAsTable(partitionTable)

    // Partitioned column `key` will be moved to the end of the schema.
    sql("SELECT * FROM "+partitionTable).show()

    spark.stop()
  }
}
