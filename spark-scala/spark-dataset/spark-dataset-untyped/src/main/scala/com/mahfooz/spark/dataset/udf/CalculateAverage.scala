package com.mahfooz.spark.dataset.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

object CalculateAverage extends UserDefinedAggregateFunction {

  //Aggregate function can have input arguments
  def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

  //Values in the aggregation buffer with data types
  def bufferSchema: StructType = {
    StructType(StructField("total", LongType) :: StructField("count", LongType) :: Nil)
  }


  //Mention the data type of the returned value of the aggregation function
  def dataType: DataType = DoubleType

  //Initialize the values of aggregation buffer
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //Here, the given aggregation buffer is updated with new input data given:
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  //Two aggregation buffers merged here and stores the updated buffer values to buffer1.
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //The final average calculation is done here.
  def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)

  override def deterministic: Boolean = true
}
