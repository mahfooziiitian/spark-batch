package com.mahfooz.spark.types.complex.json
import com.mahfooz.spark.types.complex.maps.SparkMaps.getClass
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_json}

object ToJson {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("ToJson")
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    df.selectExpr("(InvoiceNo, Description) as myStruct")
      .select(to_json(col("myStruct"))).show()
  }
}
