package com.mahfooz.spark.analytics.pivot
import com.mahfooz.spark.analytics.model.Student
import org.apache.spark.sql.{SparkSession, functions}

object MultiplePivotAnalysis {

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
    studentsDF
      .groupBy("graduation_year")
      .pivot("gender")
      .agg(
        functions.min("weight").as("min"),
        functions.max("weight").as("max"),
        functions.avg("weight").as("avg")
      )
      .show()
  }

}
