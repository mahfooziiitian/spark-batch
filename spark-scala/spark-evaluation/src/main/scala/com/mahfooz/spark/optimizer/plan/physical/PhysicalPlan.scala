/*

After successfully creating an optimized logical plan, Spark then begins the physical planning process.
The physical plan, often called a Spark plan, specifies how the logical plan will execute on the cluster by generating
different physical execution strategies and comparing them through a cost model.

 */
package com.mahfooz.spark.optimizer.plan.physical

import org.apache.spark.sql.SparkSession

object PhysicalPlan {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PhysicalPlan")
      .getOrCreate()


  }
}
