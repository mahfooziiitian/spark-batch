package com.mahfooz.spark.shuffling

import org.apache.spark.sql.SparkSession

object SparkShufflingDf {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .appName("json-to-map")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val simpleData = Seq(("James","Sales","NY",90000,34,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Raman","Finance","CA",99000,40,24000),
      ("Scott","Finance","NY",83000,36,19000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000)
    )

    val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")

    println(s"df partition = ${df.rdd.getNumPartitions}")

    val df2 = df.groupBy("state").count()

    println(s"df2 partition = ${df2.rdd.getNumPartitions}")

    df2.explain()

  }

}
