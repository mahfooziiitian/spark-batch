/*

While writing the PySpark DataFrame back to disk, you can choose how to partition the data based on columns by
using partitionBy() of pyspark.sql.DataFrameWriter.
This is similar to Hives partitions.

 */
package com.mahfooz.dataframe.partition.disk

import org.apache.spark.sql.SparkSession

object SparkPartitionOnDisk {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val dataHome = sys.env.getOrElse("DATA_HOME","data")

    val df = spark.read
      .format("json")
      .load(s"$dataHome/FileData/Json/2015-summary.json")

    //partitionBy()
    df.write.option("header","true")
    .partitionBy("DEST_COUNTRY_NAME")
    .mode("overwrite")
    .csv(s"$dataHome/Processing/Spark/Partition/disk-partition.csv")

  }
}
