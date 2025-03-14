package com.mahfooz.spark.hive.partition.pruning

import com.mahfooz.spark.hive.common.SparkHiveCommon

object PartitionPruningOne {

  def main(args: Array[String]): Unit = {

    val tableName="hive_df"
    val spark = SparkHiveCommon.createSparkSession("PartitionPruningOne")
    import spark.sql

    // Read data from the Hive managed table into dataframe
    val q = sql(s"""SELECT * FROM $tableName WHERE partition_bin=0""")
    q.explain(extended = true)

    spark.stop()
  }
}
