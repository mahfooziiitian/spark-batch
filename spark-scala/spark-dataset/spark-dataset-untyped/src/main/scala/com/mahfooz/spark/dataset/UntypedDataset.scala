/*
In essence, within the Structured APIs, there are two more APIs, the “untyped” DataFrames and the “typed”
Datasets.
To say that DataFrames are untyped is slightly inaccurate; they have types, but Spark maintains them completely
and only checks whether those types line up to those specified in the schema at runtime.
Datasets, on the other hand, check whether types conform to the specification at compile time.
Datasets are only available to Java Virtual Machine (JVM)–based languages (Scala and Java) and we
specify types with case classes or Java beans.
To Spark (in Scala), DataFrames are simply Datasets of Type Row
The "Row" type is Spark’s internal representation of its optimized in-memory format for computation.
This format makes for highly specialized and efficient computation because rather than using JVM types,
which can cause high garbage-collection and object instantiation costs,
Spark can operate on its own internal format without incurring any of those costs.
 */
package com.mahfooz.spark.dataset

class UntypedDataset {

}
