package com.mahfooz.spark.df.schema

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object MovieSchema {

  val movieSchema = StructType(
    Array(StructField("actor_name", StringType, true),
    StructField("movie_title", StringType, true),
    StructField("produced_year", LongType, true)))
}
