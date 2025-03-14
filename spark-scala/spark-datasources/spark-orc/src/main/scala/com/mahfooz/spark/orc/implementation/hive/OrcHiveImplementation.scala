/*

hive implementation is designed to follow Hive's behavior and uses Hive SerDe.
For example, historically, native implementation handles CHAR/VARCHAR with Spark’s native String while
hive implementation handles it via Hive CHAR/VARCHAR. The query results are different.
Since Spark 3.1.0, SPARK-33480 removes this difference by supporting CHAR/VARCHAR from Spark-side.
 */
package com.mahfooz.spark.orc.implementation.hive

import org.apache.spark.sql.SparkSession

object OrcHiveImplementation {

  def main(args: Array[String]): Unit = {

    val dataHome = sys.env.getOrElse("DATA_HOME", "orc") + "/FileData/Orc"
    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse")
    val sparkMetastoreDb = sys.env.getOrElse("derby.system.home", "")
    System.setProperty("derby.system.home", sparkMetastoreDb)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .config("spark.sql.orc.impl", "hive")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    val dataPath = s"${dataHome}/users.orc"
    val users = spark.read.orc(dataPath)
    users.printSchema
    users.show(5)
    spark.stop()
  }
}
