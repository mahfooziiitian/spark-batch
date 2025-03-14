package com.spark.cassandra.rdd

import com.datastax.spark.connector._
import com.spark.cassandra.model.Books
import org.apache.spark.{SparkConf, SparkContext}



object SparkCassandraConvert {
  
    def main(args: Array[String]): Unit = {
      
       val conf = new SparkConf()
      conf.set("spark.cassandra.connection.host", "localhost")
      conf.setMaster("local[*]")
      conf.setAppName("Spark Cassandra Integration")
      
     
      val sc = new SparkContext(conf)
      val books = sc.cassandraTable[Books]("popularbooks","books_by_author")
	                  .select("author_name","book_name","publish_year","rating","genres")
	              
	                  
	    val filteredBooks = books.filter { book  => book.genres.contains("Crime")}
      filteredBooks.map{book =>
                              book.author_name+":"+
                              book.book_name+
                              "("+ book.publish_year+")"+
                               "["+book.rating.getOrElse("No Rating")+"]"
                    }.collect.foreach(println)
    }
}