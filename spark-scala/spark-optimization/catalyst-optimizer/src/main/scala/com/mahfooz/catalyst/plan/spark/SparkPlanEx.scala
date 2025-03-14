package com.mahfooz.catalyst.plan.spark

import org.apache.spark.sql.SparkSession

object SparkPlanEx {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .appName("spark-plan")
      .master("local[*]")
      .getOrCreate()

    import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS

    spark.sessionState.conf.setConf(SHUFFLE_PARTITIONS, 10)

    import spark.implicits._

    val nums = spark.
      range(start = 0, end = 20, step = 1, numPartitions = 4).
      repartition($"id" % 8)

    println(nums.queryExecution.sparkPlan)

    val showElements = (it: Iterator[java.lang.Long]) => {
      val ns = it.toSeq
      import org.apache.spark.TaskContext
      val pid = TaskContext.get.partitionId
      println(s"[partition: $pid][size: ${ns.size}] ${ns.mkString(" ")}")
    }

    nums.foreachPartition(showElements)

    println(spark.sessionState.conf.limitScaleUpFactor)
    nums.take(13).foreach(record => print(record+" "))

  }

}
