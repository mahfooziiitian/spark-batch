/*

 */
package com.mahfooz.spark.hive.config.hdfs

import com.mahfooz.spark.hive.common.SparkHiveCommon

object HdfsHiveConfig {

  def main(args: Array[String]): Unit = {

    val spark=SparkHiveCommon.createSparkDynamicSession("HdfsHiveConfig")
    //assert(spark.conf.get("spark.sql.warehouse.dir").startsWith("hdfs"))

    val tableName = "hive_df"
    assert(spark.table(tableName).collect.length == 2)

    //Use the default value of spark.sql.hive.convertMetastoreParquet
    assert(spark.conf.get("spark.sql.hive.convertMetastoreParquet").toBoolean)

    val parts = spark
      .sharedState
      .externalCatalog
      .listPartitions("default", tableName)

    parts.foreach(println)

    spark.stop()

  }
}
