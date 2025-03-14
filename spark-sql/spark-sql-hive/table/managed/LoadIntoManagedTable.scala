package com.mahfooz.spark.sqlhive.table.managed

import org.apache.spark.sql.{SaveMode, SparkSession}

object LoadIntoManagedTable {

  def main(args: Array[String]): Unit = {

    // warehouseLocation points to the default location for managed databases and tables
    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

    println(sparkWarehouse)

    System.setProperty("derby.system.home", sys.env.getOrElse("derby.system.home",""))

    val spark = SparkSession
      .builder()
      .appName("spark hive managed table")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    // Create a Hive managed Parquet table, with HQL syntax instead of the
    //Spark SQL native syntax `USING hive`
    sql("CREATE TABLE hive_records(key int, value string) STORED AS PARQUET")

    // Save DataFrame to the Hive managed table
    val df = spark.table("src")

    df.write.mode(SaveMode.Overwrite)
      .saveAsTable("hive_records")

    // After insertion, the Hive managed table has data now
    sql("SELECT * FROM hive_records").show()
  }
}
