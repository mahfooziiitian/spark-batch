package com.mahfooz.dataframe.optimization.explain

import org.apache.spark.sql.SparkSession

object ExplainPlanDf {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("explain-df")
      .master("local[*]")
      .getOrCreate()
    val df1= spark.range(2, 10000000, 2)
    val df2 = spark.range(2, 10000000, 4)
    val step1 = df1.repartition(5)
    val step2 = step1.selectExpr("id * 5 as id")

    val step12 = df2.repartition(6)

    //Join
    val step3 = step2.join(step12, "id")
    val step4 = step3.selectExpr("sum(id)")

    step4.explain()

    spark.stop()
  }

}
