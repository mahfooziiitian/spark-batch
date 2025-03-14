package com.mahfooz.spark.arango

import com.arangodb.spark.{ArangoSpark, WriteOptions}
import com.arangodb.{ArangoDB, ArangoDBException}
import com.mahfooz.spark.arango.model.Movie
import org.apache.spark.{SparkConf, SparkContext}

object ArangoSparks {

  private val COLLECTION_NAME = "malam"
  private var arangoDB: ArangoDB = null;

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ArangoSpark")
      .set("arangodb.host", "10.133.209.62:8529")
      .set("arangodb.user", "root")
      .set("arangodb.password", "")

    val sc = new SparkContext(conf)

    val rdd = ArangoSpark.load[Movie](sc, "Movie")
    val rdd2 = rdd.filter { x =>
      x.title.matches(".*Lord.*Rings.*")
    }

    val writeOptions = new WriteOptions()
      .database("malam")
      .acquireHostList(true)
      .hosts("10.133.209.62:8529")
      .user("malam")
      .password("malam")

    ArangoSpark.save(rdd2, COLLECTION_NAME, writeOptions)

    sc.stop()
  }

}
