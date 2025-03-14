package com.mahfooz.spark.smallfile.compaction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

class CompactionJob {

  def bucket_cal(path: String, hdfsUriArg: String): Long = {
    val hdfsUri = hdfsUriArg
    val hdpConf = new Configuration
    hdpConf.set("fs.defaultFS", hdfsUri)
    val fs = FileSystem.get(hdpConf)
    val ri = fs.listFiles(new Path(path), true)
    val it = new Iterator[org.apache.hadoop.fs.LocatedFileStatus]() {
      override def hasNext = ri.hasNext

      override def next() = ri.next()
    }
    val files = it.toList
    val size = (files.map(_.getLen).sum)
    val filesize = fs.getBlockSize(new Path(path))
    val bucket = size / filesize
    (bucket + 1)
  }

  def main(args: Array[String]) {
    val tableName = args(0)
    val tablePath = args(1)
    val hdfsUri = args(2)
    val backupNeeded = args(4)
    val spark = SparkSession
      .builder
      .appName("Compaction Job for " + tableName)
      .enableHiveSupport()
      .getOrCreate()
    if (backupNeeded.toString() == "yes") {
      spark.sql("CREATE TABLE " + tableName + "_bkp IF NOT EXISTS LIKE " + tableName)
      val dfNew = spark.read.table(tableName)
      dfNew.coalesce(bucket_cal(tablePath, hdfsUri).toInt).write.mode("overwrite").insertInto(tableName)
      val dfBackup = spark.read.table(tableName + "_bkp")
      dfBackup.coalesce(bucket_cal(tablePath, hdfsUri).toInt).write.mode("overwrite").insertInto(tableName)
    } else {
      val df = spark.read.table(tableName)
      df.cache
      df.show(1)
      df.coalesce(bucket_cal(tablePath, hdfsUri).toInt).write.mode("overwrite").insertInto(tableName)
    }
  }
}
