/*

native implementation is designed to follow Spark’s data source behavior like Parquet.

 */
package com.mahfooz.spark.orc.implementation.natives

import org.apache.spark.sql.SparkSession

object OrcNativeImplementation {

  def main(args: Array[String]): Unit = {

    val dataHome = sys.env.getOrElse("DATA_HOME", "orc") + "/FileData/Orc"
    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .config("spark.sql.orc.impl", "native")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val dataPath = s"${dataHome}/users.orc"
    val users = spark.read.orc(dataPath)
    users.printSchema
    users.show(5)
    spark.stop()
  }

}
