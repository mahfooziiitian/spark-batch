/*

Spark makes its cluster computing capabilities available to an application in the form of a library.

This library is written in Scala, but it provides an application programming interface (API) in multiple languages.

At the time this book is being written, the Spark API is available in Scala, Java, Python, and R.

The Spark API consists of two important abstractions: SparkContext and Resilient Distributed Datasets (RDDs).

DataFrames released in Spark 1.3 on 2013.
DataSet released in Spark 1.6 on 2015.

DataFrames is an organized form of distributed data into named columns that is similar to a table in a relational database.
Dataset is an upgraded release of DataFrame that includes the functionality of object-oriented programming interface, 
type-safe and fast

In DataFrames, data is represented as a distributed collection of row objects.
In DataSets, data is represented as rows internally and JVM Objects externally.

After RDD transformation into dataframe, it cannot be regenerated to its previous form.
After RDD transformation into a dataset, it is capable of converting back to original RDD.


In dataframe, a runtime error will occur while accessing a column that is not present in the table. It does not provide compile-time type safety.
In dataset, compile time error will occur in the same scenario as dataset provides the compile-time type safety.


The Dataframe API provides a Tungsten execution backend that handles the memory management explicitly and generates bytecode dynamically.
The Dataset API provides the encoder that handles the conversion from JVM objects to table format using Spark internal Tungsten binary format.

Dataframe supports the programming languages such as Java, Python, Scala, and R.
Dataset supports Scala and Java only.


 */
package spark.api;

public class Api {
}
