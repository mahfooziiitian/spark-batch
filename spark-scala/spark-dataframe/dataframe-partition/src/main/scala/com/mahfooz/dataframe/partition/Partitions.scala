/*

To allow every executor to perform work in parallel, Spark breaks up the data into chunks called partitions.
A partition is a collection of rows that sit on one physical machine in your cluster.
A DataFrame’s partitions represent how the data is physically distributed across the cluster of machines during
execution.
If you have one partition, Spark will have a parallelism of only one, even if you have thousands of executors.
If you have many partitions but only one executor, Spark will still have a parallelism of only one because there is
only one computation resource.
An important thing to note is that with DataFrames you do not (for the most part) manipulate partitions manually or individually.
You simply specify high-level transformations of data in the physical partitions, and Spark determines how this
work will actually execute on the cluster.

 */
package com.mahfooz.dataframe.partition

import org.apache.spark.sql.SparkSession

object Partitions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .config("spark.sql.warehouse.dir","D:\\data\\processing\\spark-warehouse")
      .getOrCreate()

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
    import spark.implicits._
    val dataFrameWay = flightData2015
      .groupBy('DEST_COUNTRY_NAME)
      .count()
      .show()
  }
}
