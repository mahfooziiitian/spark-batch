package com.mahfooz.spark.analytics.pivot

import com.mahfooz.spark.analytics.model.Student
import org.apache.spark.sql.SparkSession

object PivotAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("PivotAnalysis")
      .master("local[*]")
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
    // calculating the average weight for each gender per graduation year
    studentsDF
      .groupBy("graduation_year")
      .pivot("gender")
      .avg("weight")
      .show()
  }
}
