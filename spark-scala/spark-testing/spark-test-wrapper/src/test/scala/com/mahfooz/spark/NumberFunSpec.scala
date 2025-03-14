package com.mahfooz.spark

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import com.mahfooz.spark.wrapper.SparkSessionTestWrapper

class NumberFunSpec
    extends AnyFunSpec
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  it("appends an is_even column to a Dataframe") {

    val sourceDF = Seq(
      (1),
      (8),
      (12)
    ).toDF("number")

    val actualDF = sourceDF
      .withColumn("is_even", NumberFun.isEvenUDF(col("number")))

    val expectedSchema = List(
      StructField("number", IntegerType, false),
      StructField("is_even", BooleanType, false)
    )

    val expectedData = Seq(
      Row(1, false),
      Row(8, true),
      Row(12, true)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assertSmallDataFrameEquality(actualDF, expectedDF)

  }
}
