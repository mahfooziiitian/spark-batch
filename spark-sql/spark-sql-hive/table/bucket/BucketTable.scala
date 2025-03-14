package com.mahfooz.spark.sqlhive.table.bucket

case class Student (id: Int, name:String, age:Int)

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File

object BucketTable {

  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File(sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")).getAbsolutePath
    System.setProperty("derby.system.home",sys.env.getOrElse("derby.system.home","metastore_db"))

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("BucketTable")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val studentDF = Seq(
      Student(1,"Mahfooz",37),
      Student(2,"Shziya",32),
      Student(3,"Hamdan",4),
      Student(4,"Hadiya",4)
    ).toDF

    spark.sql("""
        |CREATE TABLE partition_db.student (id INT, name STRING, age INT)
        |    USING CSV
        |    PARTITIONED BY (age)
        |    CLUSTERED BY (Id) INTO 4 buckets
        |""".stripMargin)

    studentDF.write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .partitionBy("age")
      .saveAsTable("partition_db.student")
  }
}
