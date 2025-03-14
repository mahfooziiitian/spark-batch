package com.mahfooz.spark.checkpoint

import org.apache.spark.RecoverCheckpoint
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.rand

object Checkpoint {

  val dataHome= sys.env.getOrElse("DATA_HOME","data")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("spark-checkpoint")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val nums = spark.range(5)
      .withColumn("random",
        rand()).filter($"random" > 0.5)

    nums.show

    println(nums.queryExecution.toRdd.toDebugString)
    spark.sparkContext.setCheckpointDir(s"${dataHome}\\spark\\checkpoints")

    val checkpointDir = spark.sparkContext.getCheckpointDir.get
    println(checkpointDir)

    val numsCheckpoint = nums.checkpoint
    println(numsCheckpoint.queryExecution.toRdd.toDebugString)

    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org.apache.spark.rdd.ReliableRDDCheckpointData")
      .setLevel(Level.INFO)

    println(nums.checkpoint)

    val schema = nums.schema

    import org.apache.spark.sql.execution.LogicalRDD
    val logicalRDD = numsCheckpoint.queryExecution.optimizedPlan.asInstanceOf[LogicalRDD]
    val checkpointFiles = logicalRDD.rdd.getCheckpointFile.get
    println(checkpointFiles)

    val numsRddRecovered = RecoverCheckpoint.recover[InternalRow](spark.sparkContext,
      checkpointFiles)

    numsRddRecovered.foreach(println)

  }

}
