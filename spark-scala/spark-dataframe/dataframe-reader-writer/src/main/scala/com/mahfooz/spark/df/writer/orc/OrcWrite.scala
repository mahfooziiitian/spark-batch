package com.mahfooz.spark.df.writer.orc

import org.apache.spark.sql.SparkSession

object OrcWrite {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("OrcWrite")
      .getOrCreate()

    val orcFile = spark.read
      .format("orc")
      .load(getClass.getResource("/2010-summary.orc").getFile)

    orcFile.write
      .format("orc")
      .mode("overwrite")
      .save("/tmp/my-json-file.orc")
  }
}
