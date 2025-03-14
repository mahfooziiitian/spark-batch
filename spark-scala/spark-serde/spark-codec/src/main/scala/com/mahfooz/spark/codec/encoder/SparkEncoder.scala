package com.mahfooz.spark.codec.encoder

import org.apache.spark.sql.SparkSession

object SparkEncoder {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkEncoder")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val dataset = spark.range(5)

    dataset.printSchema()

    import spark.implicits._

    // Variant 1: filter operator accepts a Scala function
    dataset.filter(n => n % 2 == 0).count

    spark.range(1).filter('id === 0).explain(true)

    // Variant 2: filter operator accepts a Column-based SQL expression
    dataset.filter('id % 2 === 0).count

    // Variant 3: filter operator accepts a SQL query
    dataset.filter("id % 2 = 0").count

    spark.range(1).filter(_ == 0).explain(true)

  }

}
