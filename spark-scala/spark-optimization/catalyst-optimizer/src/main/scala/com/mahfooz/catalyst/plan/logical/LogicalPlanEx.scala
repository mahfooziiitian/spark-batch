/**
 * A logical plan is a tree that represents both schema and data.
 * These trees are manipulated and optimized by a catalyst framework.
 *
 */
package com.mahfooz.catalyst.plan.logical

import org.apache.spark.sql.SparkSession

object LogicalPlanEx {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CatalogMetastore")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    import spark.implicits._

    val nums = Seq((0, "zero"), (1, "one")).toDF("id", "name")
    val numsSorted = nums.sort('id.desc, 'name)
    val logicalPlan = numsSorted.queryExecution.logical
    println(logicalPlan.numberedTreeString)
  }

}
