package com.mahfooz.spark.dataset.schema.view

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,desc,window}

object View {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .master("local")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val staticDataFrame = spark.read.
      format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(0))

    staticDataFrame.createOrReplaceTempView("retail_data")
    val staticSchema = staticDataFrame.schema
    println(staticSchema)

    staticDataFrame
      .selectExpr( "CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
      .groupBy(col("CustomerId"), window(col("InvoiceDate"),"1 day"))
      .sum("total_cost")
      .orderBy(desc("sum(total_cost)"))
      .take(5)

    val streamingDataFrame = spark.readStream
      .schema(staticSchema)
      .option("maxFilesPerTrigger", 1)
      .format("csv")
      .option("header", "true")
      .load(args(0))


  }
}
