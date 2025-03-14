package com.mahfooz.dataframe.schema

import org.apache.spark.sql.SparkSession

object SchemaWithColumnRenamed {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().master("local[*]").appName("FrescoPlayTest").getOrCreate()

    val langPercentDF=spark.createDataFrame(List(("Scala",35),("Python",30),
      ("R",15),("Java",20)))

    langPercentDF.show()

    val lpDF=langPercentDF.withColumnRenamed("_1","language")
      .withColumnRenamed("_2","percent")

    lpDF.show()

  }
}
