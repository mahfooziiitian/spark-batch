package com.mahfooz.dataframe.columns.rename

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object RenamingColumn {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(RenamingColumn.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("json")
      .load(args(0))

    df.withColumn("numberOne", lit(1))
      .show(2)

    df.withColumnRenamed("DEST_COUNTRY_NAME", "dest")
      .show()

  }
}
