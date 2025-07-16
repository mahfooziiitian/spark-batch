package com.mahfooz.spark.xml

import org.apache.spark.sql.SparkSession

object SparkXml {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("spark-xml")
      .getOrCreate()

    val filePath = "" //os. Path(sys.env.getOrElse("DATA_HOME","data")) / "file_data" /"xml" /"IdentUebersetzungen.xml"

    val df = spark.read.format("com.databricks.spark.xml")
      .option("rowTag", "IdentUebersetzung")
      .option("rootTag", "IdentUebersetzung")
      .option("attributePrefix","attr_")
      .load(filePath)

    df.printSchema()

    val newDf = df.selectExpr("Attr_IdentUebersetzungName",
      "explode(Lables.Lable) as Lable")

    newDf.selectExpr("Attr_IdentUebersetzungName as IdentUebersetzungName",
      "Lable.Attr_LableName as LableName",
      "Lable.Attr_ServiceShortName as ServiceShortName").show(truncate=false)

  }

}
