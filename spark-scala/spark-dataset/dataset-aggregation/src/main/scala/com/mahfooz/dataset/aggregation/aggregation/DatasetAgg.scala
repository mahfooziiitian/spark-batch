package com.mahfooz.dataset.aggregation.aggregation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum}

object DatasetAgg {

  val sparkWarehouse: String = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DatasetAgg")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    //agg on a Dataset is simply a shortcut for groupBy().agg()
    //agg can compute aggregate expressions on all the records in a Dataset.
    spark.range(10).agg(sum(col("id")) as "sum").show()
  }

}
