/*

 */
package com.mahfooz.spark.hive.metastore

import org.apache.spark.sql.SparkSession

object SparkHiveMetastore {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .master("local")
      .appName("SparkHiveMetastore")
      .config("spark.sql.warehouse.dir","/user/hive/warehouse")
      .config("hive.metastore.uris","thrift://mtmdevhdopms01.intl.vsnl.co.in:9083")
      .enableHiveSupport()
      .getOrCreate()

    val dbs=spark.sqlContext.sql("show databases")

    dbs.show()
    spark.stop()
  }

}
