package com.mahfooz.spark.rdd.partition.foreach

import org.apache.spark.sql.SparkSession

import java.sql.DriverManager

object ForEachPartition {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getName)
      .getOrCreate()

    val rdd = spark.range(1000).repartition(8).rdd

    rdd.foreachPartition {
      rddPartition => {
        val thinUrl = "jdbc:mysql://localhost:3306/spark?user=root&password=MySQL_2023"
        val conn = DriverManager.getConnection(thinUrl)
        conn.setAutoCommit(false)
        rddPartition.foreach {
          record =>conn.createStatement().execute("INSERT INTO spark_partition VALUES (" + record + ")")
        }
        conn.commit()
      }
    }

  }

}
