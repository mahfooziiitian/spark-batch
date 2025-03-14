package com.mahfooz.spark.hive.version

import com.mahfooz.spark.hive.common.SparkHiveCommon
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.util.VersionInfo

object HiveVersionCheck {

  def main(args: Array[String]): Unit = {
    val spark = SparkHiveCommon.createSparkSession("HiveVersionCheck")
    assert(spark.version == "2.4.0-cdh6.3.3")

    assert(VersionInfo.getVersion == "3.0.0-cdh6.3.3")

    assert(ShimLoader.getMajorVersion == "0.23")

    println(spark.conf.get("spark.sql.catalogImplementation"))

  }
}
