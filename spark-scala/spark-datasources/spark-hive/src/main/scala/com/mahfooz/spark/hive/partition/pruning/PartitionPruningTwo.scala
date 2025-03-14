package com.mahfooz.spark.hive.partition.pruning

import com.mahfooz.spark.hive.common.SparkHiveCommon
import org.apache.spark.sql.execution.FileSourceScanExec

object PartitionPruningTwo {

  def main(args: Array[String]): Unit = {

    val tableName = "hive_df"
    val spark = SparkHiveCommon.createSparkSession("PartitionPruningTwo")
    import spark.sql

    // Read data from the Hive managed table into dataframe
    val q = sql(s"""SELECT * FROM $tableName WHERE partition_bin=0""")

    val scan = q.queryExecution.executedPlan.collect {
      case op: FileSourceScanExec => op
    }.head
    val index = scan.relation.location
    println(
      s"Time of partition metadata listing: ${index.metadataOpsTimeNs.get}ns"
    )

    // You may also want to review metadataTime metric in web UI
    // Includes the above time and the time to list files
    // You should see the following value (YMMV)
    scan.execute.collect
    println(scan.metrics("metadataTime").value)
  }
}
