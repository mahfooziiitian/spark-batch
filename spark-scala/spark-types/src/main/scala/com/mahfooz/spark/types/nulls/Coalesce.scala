package com.mahfooz.spark.types.nulls
import com.mahfooz.spark.types.filter.FiltersColumns.getClass
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}
object Coalesce {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Coalesce")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    df.select(coalesce(col("Description"),
      col("CustomerId"))).show()
  }

}
