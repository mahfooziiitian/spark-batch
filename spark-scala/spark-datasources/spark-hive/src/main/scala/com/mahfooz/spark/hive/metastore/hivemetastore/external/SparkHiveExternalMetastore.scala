/*
When SparkSession is created with Hive support the external catalog (aka metastore) is HiveExternalCatalog.
HiveExternalCatalog uses

  a)  spark.sql.warehouse.dir
      The directory for the location of the databases.

  b)  javax.jdo.option
      This properties for the connection to the Hive metastore database.
      The metadata of relational entities is persisted in a metastore database over JDBC and DataNucleus
      AccessPlatform that uses javax.jdo.option properties.

 */
package com.mahfooz.spark.hive.metastore.hivemetastore.external

import com.mahfooz.spark.hive.common.SparkHiveCommon

object SparkHiveExternalMetastore {



  def main(args: Array[String]): Unit = {
    val sparkSession=SparkHiveCommon.createSparkSession("SparkHiveExternalMetastore")
    val externalCatalog=sparkSession.sharedState.externalCatalog
    println(sparkSession.sharedState.warehousePath)
    println(sparkSession.sparkContext.hadoopConfiguration)
  }
}
