package com.mahfooz.spark.xml.tag.root

import org.apache.spark.sql.SparkSession

object SparkReadRootTagXml {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("xsd-to-schema")
      .master("local[*]")
      .getOrCreate()

    val filePath = sys.env.getOrElse("DATA_HOME","data")+"\\FileData\\Xml\\books.xml"

    val df = spark.read.format("com.databricks.spark.xml")
      .option("rowTag", "catalog")
      .option("rootTag", "catalog")
      .load(filePath)

    df.printSchema()
    val bookDf = df.selectExpr("dt_creation","explode(book) as book")
    bookDf.printSchema()
    bookDf.show(truncate = false)

  }

}
