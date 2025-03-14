package com.mahfooz.df.aggregation.pivot

import com.mahfooz.datafram.aggregation.model.Student
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PivotAggregationUsingFilter {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PivotAggregationUsingFilter")
      .getOrCreate()

    import spark.implicits._

    val studentsDF = Seq(
      Student("John", "M", 180, 2015),
      Student("Mary", "F", 110, 2015),
      Student("Derek", "M", 200, 2015),
      Student("Julie", "F", 109, 2015),
      Student("Allison", "F", 105, 2015),
      Student("kirby", "F", 115, 2016),
      Student("Jeff", "M", 195, 2016)
    ).toDF

    studentsDF.groupBy("graduation_year").pivot("gender", Seq("M"))
      .agg(
        min("weight").as("min"),
        max("weight").as("max"),
        avg("weight").as("avg")
      ).show()
  }

}
