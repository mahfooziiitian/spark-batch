package com.spark.cassandra.model

/**
 * Save Books that belongs to Crime to below Table
 *
 * CREATE TABLE crime_books (
 * author_name TEXT,
 * publish_year INT,
 * book_name TEXT,
 * rating FLOAT,
 * PRIMARY KEY((author_name),publish_year)
 * )
 */


case class BooksInfo(author_name: String, book_name: String, publish_year: Int, rating: Option[Float], genres: Set[String])
