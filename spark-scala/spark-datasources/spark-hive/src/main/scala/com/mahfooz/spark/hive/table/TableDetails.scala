/*

hdfs://mtmdevhdopms01.intl.vsnl.co.in:8020/user/hdfs/malam/kv1.txt

 */
package com.mahfooz.spark.hive.table

import com.mahfooz.spark.hive.common.SparkHiveCommon

object TableDetails {

  def main(args: Array[String]): Unit = {

    val tableName=args(0)
    val spark =SparkHiveCommon.createSparkSession("TableDetails")
    import spark.sql
    sql(s"DESCRIBE EXTENDED $tableName").show(Integer.MAX_VALUE, truncate = false)
    spark.stop()
  }
}
