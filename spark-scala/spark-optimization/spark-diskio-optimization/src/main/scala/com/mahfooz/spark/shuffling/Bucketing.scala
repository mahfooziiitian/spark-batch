package com.mahfooz.spark.shuffling

import org.apache.spark.sql.SparkSession

object Bucketing {

    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

    def main(args: Array[String]): Unit = {

      val spark = SparkSession
        .builder()
        .config("spark.master", "local[*]")
        .appName("Bucketing")
        .config("spark.sql.warehouse.dir", sparkWarehouse)
        .enableHiveSupport()
        .getOrCreate()

      println(s"spark.sql.sources.bucketing.enabled = " +
        s"${spark.conf.get("spark.sql.sources.bucketing.enabled")}")

      println(s"spark.sql.sources.bucketing.maxBuckets = " +
        s"${spark.conf.get("spark.sql.sources.bucketing.maxBuckets")}")

      //println(s"spark.sql.sources.bucketing.autoBucketedScan.enabled = " +
      //  s"${spark.conf.get("spark.sql.sources.bucketing.autoBucketedScan.enabled")}")

      println(s"spark.sql.bucketing.coalesceBucketsInJoin.maxBucketRatio = " +
        s"${spark.conf.get("spark.sql.bucketing.coalesceBucketsInJoin.maxBucketRatio")}")

      println(s"spark.sql.legacy.bucketedTableScan.outputOrdering = " +
        s"${spark.conf.get("spark.sql.legacy.bucketedTableScan.outputOrdering")}")

      println(s"spark.sql.shuffle.partitions = " +
        s"${spark.conf.get("spark.sql.shuffle.partitions")}")

    }

}
