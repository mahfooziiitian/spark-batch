/*

export DATA_HOME=C:\Users\Mohammed_Alam\learning\data
set derby.system.home=C:\Users\Mohammed_Alam\learning\data\derby-db\hive_metastore
set HIVE_EXTERNAL = C:\Users\Mohammed_Alam\learning\data\spark\spark-hive-external


 */
package com.mahfooz.spark.sqlhive.table.external

import org.apache.spark.sql.{SaveMode, SparkSession}

object LoadIntoExternalTable {

  def main(args: Array[String]): Unit = {

    val warehouseLocation = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
    val derbySystemHome = sys.env.getOrElse("derby.system.home","metastore_db")

    System.setProperty("derby.system.home",derbySystemHome)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("LoadIntoExternalTable")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    // Prepare a Parquet data directory
    val dataDir = s"${sys.env.getOrElse("DATA_HOME","data")}/Processing/Spark/SparkHive/hive_bigints"
    println(s"external hive data dir : $dataDir")

    spark.range(10).write
      .mode(SaveMode.Overwrite)
      .parquet(dataDir)

    // Create a Hive external Parquet table
    // This accepts only unix style file format
    sql(
      s"CREATE EXTERNAL TABLE  if not exists hive_bigints(id bigint) " +
        s"STORED AS PARQUET LOCATION " +
        s"'$dataDir'"
    )
    // The Hive external table should already have data
    sql("SELECT * FROM hive_bigints").show()

    sql("drop table hive_bigints")
  }

}
