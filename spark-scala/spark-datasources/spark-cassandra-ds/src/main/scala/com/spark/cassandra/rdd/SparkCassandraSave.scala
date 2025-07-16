package com.spark.cassandra.rdd

import com.datastax.spark.connector._
import com.spark.cassandra.model.BooksInfo
import org.apache.spark.{SparkConf, SparkContext}

object SparkCassandraSave {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.set("spark.cassandra.connection.host", "localhost")
    conf.setMaster("local[*]")
    conf.setAppName("Spark Cassandra Integration")


    val sc = new SparkContext(conf)
    val books = sc.cassandraTable[BooksInfo]("popularbooks",
      "books_by_author")
      .select("author_name", "book_name", "publish_year", "rating", "genres")


    val crime_Books = books.filter(book => book.genres.contains("Crime"))
    crime_Books.saveToCassandra("popularbooks", "crime_books", SomeColumns("author_name", "publish_year", "book_name", "rating"))

  }
}