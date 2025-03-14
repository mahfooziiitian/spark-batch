package com.everestre.xsd.schema

import com.databricks.spark.xml.util.XSDToSchema
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

import java.nio.file.Paths

object SparkSchemaToJsonFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark_schema").master("local[*]").getOrCreate()
    val dataHome = sys.env.getOrElse("DATA_HOME","/")
    val xsdPath = (dataHome + "\\FileData\\Xml\\notes.xsd").replace("\\","/")
    val rowTagExpression = "note.*"
    val jsonSchema = sparkSchemaFromXsd(spark, xsdPath, rowTagExpression)

    println(jsonSchema.prettyJson)

  }

  private def sparkSchemaFromXsd(spark: SparkSession, xsdPath: String, rowTagExpression: String): StructType = {
    val schema = spark
      .createDataFrame(
        spark
          .sparkContext
          .emptyRDD[Row],
        XSDToSchema
          .read(Paths.get(xsdPath))
      )
      .select(rowTagExpression)
      .schema
    schema
  }
}
