package com.mahfooz.spark.rdd.partition.partitioners.custom

import org.apache.spark.Partitioner

class CustomPartitioner(numberOfPartition: Int) extends Partitioner{

  override def numPartitions: Int = numberOfPartition

  override def getPartition(key: Any): Int = {
    Math.abs(key.asInstanceOf[String].hashCode() % numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case partitioner: CustomPartitioner => partitioner.numPartitions == numPartitions
    case _ =>  false
  }
}
