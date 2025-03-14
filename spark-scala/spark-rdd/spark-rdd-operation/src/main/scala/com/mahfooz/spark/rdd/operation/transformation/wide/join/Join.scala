/*

By joining the dataset of type (K,V) and dataset (K,W), the result of the joined dataset is (K,(V,W)).

 */
package com.mahfooz.spark.rdd.operation.transformation.wide.join

import org.apache.spark.{SparkConf, SparkContext}

object Join {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(Join.getClass.getName)
      .setMaster("local")

    val sc=new SparkContext(conf)

    val memberTx = sc.parallelize(List((110, 50.35), (127, 305.2), (126, 211.0),
      (105, 6.0),(165, 31.0), (110, 40.11)))

    val memberInfo = sc.parallelize(List((110, "a"), (127, "b"), (126, "b"),
      (105, "a"),(165, "c")))

    val memberTxInfo = memberTx.join(memberInfo)

    memberTxInfo.collect().foreach(println)
  }
}
