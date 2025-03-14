/**
 * groupBy(cols: Column*): RelationalGroupedDataset
 * groupBy(col1: String, cols: String*): RelationalGroupedDataset
 */
package com.mahfooz.dataset.aggregation.untyped.groupby

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

object GroupByEx {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("GroupByEx")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    import spark.implicits._

    val ints = 1 to math.pow(10, 3).toInt

    val nms = ints.toDS().
      withColumnRenamed("value","n").
      withColumn("m", 'n % 2)

    val q = nms.
      groupBy('m).
      agg(sum('n) as "sum").
      orderBy('m)

    q.show

  }

}
