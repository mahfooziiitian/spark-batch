/*

 */
package com.mahfooz.spark.sqlhive

import org.apache.spark.sql.SparkSession

case class Record(key: Int, value: String)

object SparkHiveSql {

  def main(args: Array[String]): Unit = {

    val dataHome = sys.env.getOrElse("DATA_HOME","")+"/spark"
    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
    System.setProperty("derby.system.home", sys.env.getOrElse("derby.system.home",""))

    val spark = SparkSession
      .builder()
      .appName("SparkHiveSql")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    val dataPath = s"${dataHome}/FileData/Text/kv1.txt"
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql(s"LOAD DATA LOCAL INPATH \'$dataPath\' INTO TABLE src")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()

    sql("drop table src")
  }
}
