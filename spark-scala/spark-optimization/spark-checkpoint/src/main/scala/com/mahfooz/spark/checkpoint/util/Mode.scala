package com.mahfooz.spark.checkpoint.util

object Mode extends Enumeration {
  type Mode = Value
  // Assigning Values
  val NO_CACHE_NO_CHECKPOINT = Value("NO_CACHE_NO_CHECKPOINT") // ID = 0
  val CACHE = Value("CACHE") // ID = 1
  val CHECKPOINT = Value("CHECKPOINT") // ID = 2
  val CHECKPOINT_NON_EAGER = Value("CHECKPOINT_NON_EAGER") // ID = 3
}