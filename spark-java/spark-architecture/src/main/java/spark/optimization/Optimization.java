/*

Optimization refers to the fine-tuning of a system to make it more efficient and to reduce its resource utilization.

Spark SQL Optimization is achieved by Catalyst Optimizer.

Catalyst Optimizer is:

The core of Spark SQL.
Provides advanced programming language features to build a query optimizer.
Based on functional programming construct in Scala.
Supports rule-based optimization (defined by a set of rules to execute the query) and cost-based optimization (defined by selecting the most suitable way to execute a query).
To manipulate the tree, the catalyst contains the tree and the set of rules.

n Spark SQL, the Catalyst performs the following functions:

Analysis
Logical Optimization
Physical Planning
Code Generation


Performance Tuning Options in Spark SQL
	
	1.spark.sql.codegen

		Used to improve the performance of large queries.
		The value of spark.sql.codegen should be true.
		The default value of spark.sql.codegen is false.

2.spark.sql.inMemorycolumnarStorage.compressed

We can compress the in-memory columnar storage automatically based on statistics of data.
The value of spark.sql.inMemorycolumnarStorage.compressed should be true.
Its default value will be false.
**3.**spark.sql.inMemoryColumnarStorage.batchSize

We can boost up memory utilization by giving the larger values for this parameter spark.sql.inMemoryColumnarStorage.batchSize.
The default value will be 10000.
**4.**spark.sql.parquet.compression.codec

We can enable high speed and reasonable compression using the parameter spark.sql.parquet.compression.codec.
The snappy library can be used for compression and decompression.



*/

package spark.optimization;

class Optimzation {

}
