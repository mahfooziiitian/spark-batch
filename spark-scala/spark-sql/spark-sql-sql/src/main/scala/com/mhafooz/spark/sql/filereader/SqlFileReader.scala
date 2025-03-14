package com.mhafooz.spark.sql.filereader

import org.apache.spark.sql.SparkSession

object SqlFileReader {

  def main(args: Array[String]): Unit = {

    val sourceFileName=s"file:///${sys.env.getOrElse("DATA_HOME","data").replace("\\","/")}/FileData/Parquet/Movies/movies.parquet"

    val sparkWarehouse =
      sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse").replace("\\", "/")
    System.setProperty(
      "derby.system.home",
      sys.env.getOrElse("derby.system.home", "").replace("\\", "/")
    )

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SqlFileReader")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    spark
      .sql("SELECT * FROM parquet.`" + sourceFileName + "`")
      .show(5)

    spark.stop()

  }
}
