/**
 * On Spark DataFrame foreachPartition() is similar to foreach() action which is used to
 * manipulate the accumulators, write to a database table or external data sources but the
 * difference being foreachPartition() gives you an option to do heavy initializations per each
 * partition and is consider most efficient.
 *
 * foreach() and foreachPartition() are action function
 *
 */
package com.mahfooz.dataframe.partition.foreachpartition

import org.apache.spark.sql.SparkSession

case class Movies (id: Int,name:String)

object ForEachPartition {

  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .appName("for-each-partition")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val ds = Seq(
      Movies(1,"Love"),
      Movies(2,"Another Love"),
      Movies(3,"Many Love"),
      Movies(4,"One Love")
    ).toDS

    println(ds.rdd.getNumPartitions)

    ds.foreachPartition((record: Iterator[Movies]) => {
      println(record.length)
    })
  }

}
