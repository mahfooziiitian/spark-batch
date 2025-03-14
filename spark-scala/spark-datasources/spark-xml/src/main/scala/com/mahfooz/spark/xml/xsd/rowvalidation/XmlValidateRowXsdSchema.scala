package com.mahfooz.spark.xml.xsd.rowvalidation

import org.apache.spark.sql.SparkSession

object XmlValidateRowXsdSchema {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("xsd-to-schema")
      .master("local[*]")
      .getOrCreate()

    val xmlFile = sys.env.getOrElse("DATA_HOME","data")+"\\FileData\\Xml\\item_exports.xml"
    val xsdFile = sys.env.getOrElse("DATA_HOME","data")+"\\FileData\\Xml\\item_exports.xsd"

    spark.sparkContext.addFile(xsdFile)

    val df = spark.read.format("xml")
      .option("rootTag", "ItemExport")
      .option("rowTag", "ItemExport")
      .option("rowValidationXSDPath","item_exports.xsd")
      .load(xmlFile)

    df.printSchema()
    df.show(false)
  }

}
