package com.mahfooz.spark.dataset.typed.typesafe

import org.apache.spark.sql.expressions.Aggregator
import com.mahfooz.spark.dataset.model.{Average, People}
import org.apache.spark.sql.{Encoder, Encoders}

object CalculateAverage extends Aggregator[People, Average, Double]{

  def zero: Average = new Average(0L,0L)

  //Intermediate values are merged here:
  def merge(b1: Average, b2: Average): Average = {
    b1.total += b2.total
    b1.count += b2.count
    b1
  }

  //Aggregation function calculation is done here:
  def finish(reduction: Average): Double = reduction.total.toDouble / reduction.count

  //Encoder for the intermediate value type is specified here:
  def bufferEncoder: Encoder[Average] = Encoders.product[Average]

  //Encoder for the final output value type is specified here:
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble


  def reduce(buffer: Average, people: People): Average = {
    buffer.total += people.age
    buffer.count += 1
    buffer
  }

}
