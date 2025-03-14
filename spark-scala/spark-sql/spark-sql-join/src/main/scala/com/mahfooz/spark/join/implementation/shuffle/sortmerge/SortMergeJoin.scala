/*

By default , Spark uses this method while joining data frames.
It’s two step process.

1.  First all executors should exchange data across network to sort and re-allocate sorted partitions.
    At the end of this stage , Each executor should have same key valued data on both data frame partitions so
    that executor can do merge operation.

2.  Merge is very quick thing.

 */
package com.mahfooz.spark.join.implementation.shuffle.sortmerge

import org.apache.spark.sql.SparkSession

object SortMergeJoin {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local[*]")
      .appName("SortMergeJoin")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    println(s"spark.sql.join.preferSortMergeJoin = ${spark.conf.get("spark.sql.join.preferSortMergeJoin")}")

    val data1 = Seq(10, 20, 20, 30, 40, 10, 40, 20, 20, 20, 20, 50)

    import spark.implicits._

    val df1 = data1.toDF("id1")
    val data2 = Seq(30, 20, 40, 50)

    val df2 = data2.toDF("id2")

    val dfJoined = df1.join(df2, $"id1" === $"id2")

    println(dfJoined.queryExecution.executedPlan)

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

    val dfJoinedSortMerge = df1.join(df2, $"id1" === $"id2")

    println(dfJoinedSortMerge.queryExecution.executedPlan)

    dfJoined.show

  }

}
