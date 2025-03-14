package com.mahfooz.dataframe.action

import org.apache.spark.sql.SparkSession

object CollectAction {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CollectAction")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val deptDF = Seq(
      ("Finance",10),
      ("Marketing",20),
      ("Sales",30),
      ("IT",40)
    ).toDF("dept_name","dept_id")

    deptDF.show(truncate=false)


    val dataCollect = deptDF.collect()
    for (elem <- dataCollect) {println(elem)}
  }
}
