/*

spark2-submit \
  --class com.mahfooz.sparksql.hive.read.ReadHiveData
  --master yarn \
  --deploy-mode client \
  sparksql-hive_2.11-1.0.jar  "malam.customers"

 */
package com.mahfooz.spark.hive.read

import com.mahfooz.spark.hive.common.SparkHiveCommon

object ReadHiveData {

  def main(args: Array[String]): Unit = {

    val tableName = args(0)

    val spark = SparkHiveCommon.createSparkSession("ReadHiveData")

    import spark.sql

    sql("SELECT * FROM " + tableName).show()

    sql("SELECT COUNT(*) FROM " + tableName).show()

    spark.stop()

  }
}
