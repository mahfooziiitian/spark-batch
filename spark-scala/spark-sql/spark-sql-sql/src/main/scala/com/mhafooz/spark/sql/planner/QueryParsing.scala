/*

Spark SQL works by parsing the SQL-like statement into an Abstract Syntax Tree (AST), subsequently converting
that plan to a logical plan and then optimizing the logical plan into a physical plan that can be executed.
The final execution uses the underlying DataFrame API, making it very easy for anyone to use DataFrame APIs by
simply using an SQL-like interface rather than learning all the internals.
Since this book dives into technical details of various APIs, we will primarily cover the DataFrame APIs, showing
Spark SQL API in some places to contrast the different ways of using the APIs.

A DataFrame is an abstraction of the Resilient Distributed dataset (RDD), dealing with higher level functions
optimized using catalyst optimizer and also highly performant via the Tungsten Initiative.
 You can think of a dataset as an efficient table of an RDD with heavily optimized binary representation of the data. The binary representation is achieved using encoders, which serializes the various objects into a binary structure for much better performance than RDD representation.
 */
package com.mhafooz.spark.sql.planner


class QueryParsing {

}
