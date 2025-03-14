package com.mahfooz.spark.join.estimation

import org.apache.spark.sql.SparkSession

object SparkJoinEstimation {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse")

    val spark = SparkSession
      .builder()
      .appName("SparkJoinEstimation")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    println(spark.sessionState.conf.cboEnabled)

    val r1 = spark.range(5)
    println(r1.queryExecution.analyzed.stats.simpleString)

    val db = spark.catalog.currentDatabase

    spark.sharedState.externalCatalog.dropTable(db, table = "t1",
      ignoreIfNotExists = true,
      purge = true)

    spark.sharedState.externalCatalog.dropTable(db, table = "t2",
      ignoreIfNotExists = true,
      purge = true)

    spark.sql("drop table if exists t1")
    spark.sql("drop table if exists t2")

    // Register tables
    spark.range(5).write.saveAsTable("t1")
    spark.range(10).write.saveAsTable("t2")

    import spark.implicits._
    import spark.sql

    // Refresh internal registries
    sql("REFRESH TABLE t1")
    sql("REFRESH TABLE t2")

    // Calculate row count stats
    val tables = Seq("t1", "t2")
    tables.map(t => s"ANALYZE TABLE $t COMPUTE STATISTICS").foreach(sql)

    val t1 = spark.table("t1")
    val t2 = spark.table("t2")

    val t1plan = t1.queryExecution.analyzed
    println(t1plan.numberedTreeString)

    val p0 = t1plan.p(0)
    println(s"Statistics of ${p0.simpleString(10)}: ${p0.stats.simpleString}")


    val p1 = t1plan.p(1)
   println(s"Statistics of ${p1.simpleString(10)}: ${p1.stats.simpleString}")


    val t2plan = t2.queryExecution.analyzed

    // let's get rid of the SubqueryAlias operator

    import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
    val t1NoAliasesPlan = EliminateSubqueryAliases(t1plan)
    val t2NoAliasesPlan = EliminateSubqueryAliases(t2plan)

    // Using Catalyst DSL
    import org.apache.spark.sql.catalyst.dsl.plans._
    import org.apache.spark.sql.catalyst.plans._
    val plan = t1NoAliasesPlan.join(
      otherPlan = t2NoAliasesPlan,
      joinType = Inner,
      condition = Some($"id".expr))

    println(plan.numberedTreeString)


    // Take Join operator off the logical plan
    // JoinEstimation works with Joins only
    import org.apache.spark.sql.catalyst.plans.logical.Join
    val join = plan.collect { case j: Join => j }.head

    // Make sure that row count stats are defined per join side
    println(join.left.stats.rowCount.isDefined)


    println(join.right.stats.rowCount.isDefined)


    // Make the example reproducible
    // Computing stats is once-only process and the estimates are cached
    join.invalidateStatsCache

    import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.JoinEstimation
    val stats = JoinEstimation(join).estimate


    // Stats have to be available so Option.get should just work
    println(stats.get.simpleString)
  }

}
