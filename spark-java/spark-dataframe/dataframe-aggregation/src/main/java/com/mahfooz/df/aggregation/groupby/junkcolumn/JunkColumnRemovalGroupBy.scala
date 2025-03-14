package com.mahfooz.df.aggregation.groupby.junkcolumn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

object JunkColumnRemovalGroupBy {

  val warehouseLocation = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
  val dataHome = sys.env.getOrElse("DATA_HOME","data")+"\\spark\\datasource\\csv"

  private def start(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("JunkColumnRemovalGroupBy")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val spark= start()
    val sc =spark.sparkContext

    import spark.implicits._

    val df = sc.parallelize(Seq(
      ("a", "foo", 1),
      ("a", "foo", 3),
      ("b", "bar", 5),
      ("b", "bar", 1)
    )).toDF("x", "y", "z")

    println(s"partition size before group by = ${df.rdd.partitions.size}")

    df.groupBy("x").agg(sum($"z")).show(false)

    scala.io.StdIn.readLine()

  }

}
