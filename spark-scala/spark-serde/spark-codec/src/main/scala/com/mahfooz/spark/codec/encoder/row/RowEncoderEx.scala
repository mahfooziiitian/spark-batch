package com.mahfooz.spark.codec.encoder.row

import org.apache.spark.sql.SparkSession

object RowEncoderEx {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
  val dataHome = sys.env.getOrElse("DATA_HOME","data")+""

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("RowEncoderEx")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()



    val df = spark.read.format("libsvm")
      .option("numFeatures", "780")
      .load("data/mllib/sample_libsvm_data.txt")
  }

}
