/*

Optimized Row Columnar (ORC) is another popular open source self-describing columnar storage format in the Hadoop ecosystem.
It was created by a company called Cloudera as part of the initiative to massively speed up Hive.

  spark.read.orc("<path>")
  spark.read.format("orc")

 */
package com.mahfooz.spark.orc

import org.apache.spark.sql.SparkSession

object OrcDemo {

  def main(args: Array[String]): Unit = {

    val dataHome = sys.env.getOrElse("DATA_HOME", "orc") + "/FileData/Orc"
    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse")

    println(s"Getting system property: ${System.getProperty("derby.system.home")}")

    System.setProperty(
      "derby.system.home",
      sys.env.getOrElse("derby.system.home", "")
    )
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val dataPath = s"${dataHome}/users.orc"
    val users = spark.read.orc(dataPath)
    users.printSchema
    users.show(5)

    spark.stop()
  }
}
