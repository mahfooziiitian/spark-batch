package com.mahfooz.spark.df.writer.json

import com.mahfooz.spark.df.schema.FlightTravelSchema
import org.apache.spark.sql.SparkSession

object JsonWrite {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("JsonWrite")
      .master("local")
      .getOrCreate()

    val csvFile=spark.read.format("json").option("mode", "FAILFAST")
      .schema(FlightTravelSchema.myManualSchema)
      .load(getClass.getResource("/2015-summary.json").getFile)

    csvFile.write.format("json")
      .mode("overwrite")
      .save("spark-dataframe/src/main/resources/2015-summary-temp.json")
  }
}
