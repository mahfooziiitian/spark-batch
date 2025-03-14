/*

Scala has some particular semantics regarding the use of == and ===.
In Spark, if you want to filter by equality you should use === (equal) or =!= (not equal).
You can also use the not function and the equalTo method.

 */
package com.mahfooz.spark.types.boolean

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Booleans {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Booleans")
      .master("local")
      .getOrCreate()

    // in Scala
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    df.where(col("InvoiceNo").equalTo(536365))
      .select("InvoiceNo", "Description")
      .show(5, false)

    df.where(col("InvoiceNo") === 536365)
      .select("InvoiceNo", "Description")
      .show(5, false)

    df.where("InvoiceNo = 536365")
      .show(5, false)

    df.where("InvoiceNo <> 536365")
      .show(5, false)

  }
}
