package com.mahfooz.spark.compression.gzip

import org.apache.spark.sql.SparkSession

object GzipCompressionParquet {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("spark-gzip-compression")
      .master("local[*]")
      .getOrCreate()

    val driverData = Seq(
    ("Alice", "A224455", "Female", 3000),
    ("Bryan","B992244","Male",4000),
    ("Catherine","C887733","Female",4000))

    import spark.implicits._

    val df = driverData.toDF("name", "license", "gender", "salary")

    val dataHome = sys.env.getOrElse("DATA_HOME","data")

    df.write.option("compression", "gzip")
      .parquet(s"${dataHome}\\spark\\compression\\customer")
  }

}
