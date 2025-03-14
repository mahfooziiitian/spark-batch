/*
Default Shuffle Partition
Calling groupBy(), union(), join() and similar functions on DataFrame results in shuffling data between multiple
executors and even machines and finally re-partitions data into 200 partitions by default.
Spark default defines shuffling partition to 200 using spark.sql.shuffle.partitions configuration.
 */
package com.mahfooz.dataframe.partition.shuffle

import org.apache.spark.sql.SparkSession

object ShufflePartition {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[5]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "5")

    import spark.implicits._

    val flightData2015 = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(args(0))

    flightData2015.take(3).foreach(println)

    flightData2015.createOrReplaceTempView("flight_data_2015")

    //SQL Ways
    val sqlWay = spark
      .sql("""SELECT DEST_COUNTRY_NAME, count(1)
                          FROM flight_data_2015
                          GROUP BY DEST_COUNTRY_NAME
                          """)
      .show()
    //DataFrame ways
    val dataFrameWay = flightData2015
      .groupBy('DEST_COUNTRY_NAME)
      .count()
      .show()
  }
}
