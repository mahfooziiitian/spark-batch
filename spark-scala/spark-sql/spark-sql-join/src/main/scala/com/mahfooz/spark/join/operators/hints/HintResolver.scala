package com.mahfooz.spark.join.operators.hints

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.analysis.ResolveHints

object HintResolver extends RuleExecutor[LogicalPlan] {
  lazy val batches = Batch(
    "Hints",
    FixedPoint(maxIterations = 100),
    //new ResolveHints.ResolveBroadcastHints(SQLConf.get),
    //ResolveHints.RemoveAllHints
  ) :: Nil

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local[*]")
      .appName("MultipleHints")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val q = spark.range(100).hint("broadcast").hint("myHint", 100, true)
    val plan = q.queryExecution.logical

    val resolvedPlan = HintResolver.execute(plan)

    println(resolvedPlan.numberedTreeString)

  }
}
