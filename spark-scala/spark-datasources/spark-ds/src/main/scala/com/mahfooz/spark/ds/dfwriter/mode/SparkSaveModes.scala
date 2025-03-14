/*

Mode          Description

append        This appends the DataFrame data to the list of files that already exist at the specified destination location.

overwrite     This completely overwrites any data files that already exist at the specified destination location with the data in the DataFrame.

error
errorIfExists
default       This is the default mode. If the specified destination location exists, then DataFrameWriter will throw an error.

ignore        If the specified destination location exists, then simply do nothing.
              In other words, silently don’t write out the data in the DataFrame.

 */
package com.mahfooz.spark.ds.dfwriter.mode

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSaveModes {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val movies = spark.read.json(args(0))

    // write data out as CVS format, but using a '#' as delimiter
    movies.write
      .format("csv")
      .option("sep", "#")
      .mode(SaveMode.Append)
      .save("C:/tmp/output/csv")

    // write data out using overwrite save mode
    movies.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("sep", "#")
      .save("C:/tmp/output/csv")

    println(movies.rdd.getNumPartitions)

    spark.stop()
  }
}
