/*

A Dataset is a new experimental interface added in Spark 1.6 that tries to provide the benefits of RDDs (strong typing, ability to use powerful
lambda functions) with the benefits of Spark SQL’s optimized execution engine.

A Dataset can be constructed from JVM objects and then manipulated using functional transformations (map, flatMap, filter, etc.).

The unified Dataset API can be used both in Scala and Java.

Python does not yet have support for the Dataset API, but due to its dynamic nature many of the benefits are already available
(i.e. you can access the field of a row by name naturally row.columnName).

Full python support will be added in a future release.

before Spark 2.0, the main programming interface of Spark was the Resilient Distributed Dataset (RDD).

After Spark 2.0, RDDs are replaced by Dataset, which is strongly-typed like an RDD, but with richer optimizations under the hood.

Spark’s primary abstraction is a distributed collection of items called a Dataset.

Dataset is known as a data structure in Spark SQL that provides type safety and object-oriented interface.

Dataset:

	Acts as an extension to DataFrame API.
	Combines the features of DataFrame and RDD.
	Serves as a functional programming interface for working with structured data.
	Overcomes the limitations of RDD and DataFrames - the absence of automatic optimization in RDD and absence of compile-time type safety in Dataframes.

Creating Datasets
	Dataset uses a specialized Encoder to serialize the objects for processing or transmitting over the network.
	
	val dsCaseClass = Seq(Person("John", 35)).toDS()
    dsCaseClass.show()
	
Dataset from RDD
A dataset can be created from RDD as shown:

    val ds=rdd.DS()
    ds.show()

Dataset from DataFrame
	
	val peopleDS = spark.read.json("examples/src/main/resources/data.json").as[Person]
	peopleDS.show()
 ===============================
 
 
Import all required packages
	
	import org.apache.spark.sql.SparkSession

Create a SparkSession object
	
	val spark= SparkSession.builder.appName("My Spark Application").master("local[*]").config("spark.sql.warehouse.dir", "/root/spark-warehouse").getOrCreate

Call the function spark.range to get the output starting from 5 to 50, with increments of 5
	
	val numDS = spark.range(5, 50, 5)

Display the value in DataSet
	numDS.show()

Call the function 'orderby' to reverse the order in DataSet and display the first 5 values
	
	numDS.orderBy(desc("id")).show(5)

Compute descriptive stats and display them
	numDS.describe().show()
	
 */
package spark.api.dataset;

public class DataSets {
}
