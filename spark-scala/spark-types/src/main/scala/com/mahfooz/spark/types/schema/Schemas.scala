package com.mahfooz.spark.types.schema

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

object Schemas {

  def main(args: Array[String]): Unit = {

    val csvLine = "0,Warsaw,Poland"

    val spark=SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    val cities: Dataset[String] = Seq(csvLine).toDS

    val schema = new StructType()
      .add($"id".long.copy(nullable = false))
      .add($"city".string)
      .add($"country".string)

    val citiesDF: DataFrame = spark
      .read
      .schema(schema)
      .csv(cities)

    citiesDF.show
  }
}