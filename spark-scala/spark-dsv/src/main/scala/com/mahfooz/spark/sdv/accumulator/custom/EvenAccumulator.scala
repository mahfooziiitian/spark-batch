package com.mahfooz.spark.sdv.accumulator.custom

import org.apache.spark.util.AccumulatorV2

class EvenAccumulator extends AccumulatorV2[BigInt, BigInt] {

  private var num:BigInt = 0

  override def reset(): Unit = {
    this.num = 0
  }

  override def add(intValue: BigInt): Unit = {
    if (intValue % 2 == 0) {
      this.num += intValue
    }
  }

  override def merge(other: AccumulatorV2[BigInt,BigInt]): Unit = {
    this.num += other.value
  }

  override def value():BigInt = {
    this.num
  }

  override def copy(): AccumulatorV2[BigInt,BigInt] = {
    new EvenAccumulator
  }

  override def isZero():Boolean = {
    this.num == 0
  }
  
}