package com.spark.cassandra.model

/**
 * Use case class
 * Retrieve all books with genre “crime”
 * Output format should be Author name: Book Name (publish year) [rating]
 *
 */


case class Books(author_name: String, book_name: String, publish_year: Int, rating: Option[Float], genres: Set[String])
