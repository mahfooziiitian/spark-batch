package com.mahfooz.spark.hive.write.paritition

import com.mahfooz.spark.hive.common.SparkHiveCommon

object InsertExistingHivePartition {

  def main(args: Array[String]): Unit = {
    val spark=SparkHiveCommon.createSparkDynamicSession("InsertExistingHivePartition")
  }
}
