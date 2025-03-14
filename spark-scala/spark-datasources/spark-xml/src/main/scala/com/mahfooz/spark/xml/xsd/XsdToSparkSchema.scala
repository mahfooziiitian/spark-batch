package com.mahfooz.spark.xml.xsd

import com.databricks.spark.xml.util.XSDToSchema
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths

object XsdToSparkSchema {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("xsd-to-schema")
      .master("local[*]")
      .getOrCreate()

    val xsdFile = sys.env.getOrElse("DATA_HOME","data")+"\\FileData\\Xml\\xsd_schema\\dwh\\DWHBatch.xsd"
    val schema = XSDToSchema.read(Paths.get(xsdFile))

    print(schema.json)
  }

}
