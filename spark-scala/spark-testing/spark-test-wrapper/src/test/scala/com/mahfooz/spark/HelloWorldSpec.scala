package com.mahfooz.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import com.mahfooz.spark.wrapper.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DataFrameComparer

class HelloWorldSpec
    extends AnyFunSpec
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  it("appends a greeting column to a Dataframe") {

    val sourceDF = Seq(
      ("miguel"),
      ("luisa")
    ).toDF("name")

    val actualDF = sourceDF.transform(HelloWorld.withGreeting())

    val expectedSchema = List(
      StructField("name", StringType, true),
      StructField("greeting", StringType, false)
    )

    val expectedData = Seq(
      Row("miguel", "hello world"),
      Row("luisa", "hello world")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assertSmallDataFrameEquality(actualDF, expectedDF)

  }

}
