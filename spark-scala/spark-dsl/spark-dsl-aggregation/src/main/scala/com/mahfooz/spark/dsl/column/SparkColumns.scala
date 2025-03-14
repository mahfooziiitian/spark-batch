package com.mahfooz.spark.dsl.column

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkColumns {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._

    val kvDF = Seq((1, 2), (2, 3)).toDF("key", "value")

    // To display column names in a DataFrame, we can call the columns function
    println(kvDF.columns)

    kvDF.select("key").show()

    kvDF.select(col("key")).show()

    kvDF.select(column("key")).show()

    kvDF.select($"key").show()

    kvDF.select('key).show()

    // using the col function of DataFrame
    kvDF.select(kvDF.col("key"))
    kvDF.select('key, 'key > 1).show
    spark.stop()
  }
}
