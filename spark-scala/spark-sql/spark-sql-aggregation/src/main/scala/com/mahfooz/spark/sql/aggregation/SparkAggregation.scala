/*

In Spark, all aggregations are done via functions.
The aggregation functions are designed to perform aggregation on a set of rows, whether that set of rows
consists of all the rows or a subgroup of rows in a DataFrame.

count(col)
  Returns the number of items per group.

countDistinct(col)
  Returns the unique number of items per group.

approx_count_distinct(col)
  Returns the approximate number of unique items per group.

min(col)
  Returns the minimum value of the given column per group.

max(col)
  Returns the maximum value of the given column per group.

sum(col)
  Returns the sum of the values in the given column per group.

sumDistinct(col)
  Returns the sum of the distinct values of the given column per group.

avg(col)
  Returns the average of the values of the given column per group.

skewness(col)
  Returns the skewness of the distribution of the values of the given column per group.

kurtosis(col)
  Returns the kurtosis of the distribution of the values of the given column per group.

variance(col)
  Returns the unbiased variance of the values of the given column per group.

stddev(col)
  Returns the standard deviation of the values of the given column per group.

collect_list(col)
  Returns a collection of values of the given column per group.
  The returned collection may contain duplicate values.

collect_set(col)
  Returns a collection of unique values per group.

 */
package com.mahfooz.spark.sql.aggregation

object SparkAggregation {

}
